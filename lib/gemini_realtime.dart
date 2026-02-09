import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:typed_data';

import 'package:logging/logging.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'audio_data_extractor.dart';

enum GeminiVoiceName { Puck, Charon, Kore, Fenrir, Aoede, Leda, Orus, Zephyr }

class GeminiRealtime {
  final _log = Logger("Gem");

  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _channelSubs;
  bool _connected = false;

  // interestingly, 'response_modalities' seems to allow only "text", "audio", "image" - not a list. Audio only is fine for us
  // Valid voices are: Puck, Charon, Kore, Fenrir, Aoede (Set to Puck, override in connect())
  // system instruction is also not set in the template map (set during connect())
  final Map<String, dynamic> _setupMap = {'setup': { 'model': 'models/gemini-2.5-flash-native-audio-preview-12-2025', 'generation_config': {'response_modalities': 'audio', 'speech_config': {'voice_config': {'prebuilt_voice_config': {'voice_name': 'Puck'}}}}, 'system_instruction': { 'parts': [ { 'text': '' } ] }}};
  final Map<String, dynamic> _realtimeAudioInputMap = {'realtimeInput': { 'mediaChunks': [{'mimeType': 'audio/pcm;rate=16000', 'data': ''}]}};
  final Map<String, dynamic> _realtimeImageInputMap = {'realtimeInput': { 'mediaChunks': [{'mimeType': 'image/jpeg', 'data': ''}]}};

  // audio buffer
  final _audioBuffer = ListQueue<Uint8List>();

  // a handle on the main app's event logger (in the UI)
  final Function(String) eventLogger;

  // a callback to notify the main app that some audio is ready for playback
  final Function() audioReadyCallback;

  /// Constructor just registers callbacks for audio ready and log messages
  GeminiRealtime(this.audioReadyCallback, this.eventLogger);

  /// Returns the current state of the Gemini connection
  bool isConnected() => _connected;

  /// Connect to Gemini Live and set up the websocket connection using the specified API key
  Future<bool> connect(String apiKey, GeminiVoiceName voice, String systemInstruction) async {
    eventLogger('Connecting to Gemini');
    _log.info('Connecting to Gemini');

    // configure the session with the specified voice and system instruction
    _setupMap['setup']['generation_config']['speech_config']['voice_config']['prebuilt_voice_config']['voice_name'] = voice.name;
    _setupMap['setup']['system_instruction']['parts'][0]['text'] = systemInstruction;

    // get the audio playback ready
    _audioBuffer.clear();

    // get a fresh websocket channel each time we start a conversation for now
    await _channel?.sink.close();
    _channel = WebSocketChannel.connect(Uri.parse('wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=$apiKey'));

    // connection doesn't complete immediately, wait until it's ready
    // TODO check what happens if API key is bad, host is bad etc, how long are the timeouts?
    // and return false if not connected properly (or throw the exception and print the error?)
    await _channel!.ready;

    // set up stream handler for channel to handle events
    _channelSubs = _channel!.stream.listen(_handleGeminiEvent);

    // set up the config for the model/modality
    _log.info(_setupMap);
    _channel!.sink.add(jsonEncode(_setupMap));

    _connected = true;
    eventLogger('Connected');
    return _connected;
  }

  /// Disconnect from Gemini Live by closing the websocket connection
  Future<void> disconnect() async {
    eventLogger('Disconnecting from Gemini');
    _log.info('Disconnecting from Gemini');
    _connected = false;
    await _channelSubs?.cancel();
    await _channel?.sink.close();
  }

  /// Sends the audio to Gemini - bytes should be provided as PCM16 samples at 16kHz
  void sendAudio(Uint8List pcm16x16) {
    if (!_connected) {
      eventLogger('App trying to send audio when disconnected');
      return;
    }

    // base64 encode
    var base64audio = base64Encode(pcm16x16);

    // set the data into the realtime input map before serializing
    // TODO can't I just cache the last little map and set it there at least?
    _realtimeAudioInputMap['realtimeInput']['mediaChunks'][0]['data'] = base64audio;

    // send data to websocket
    _channel!.sink.add(jsonEncode(_realtimeAudioInputMap));
  }

  /// Send the photo to Gemini, encoded as jpeg
  void sendPhoto(Uint8List jpegBytes) {
    if (!_connected) {
      eventLogger('App trying to send a photo when disconnected');
      return;
    }

    // base64 encode
    var base64image = base64Encode(jpegBytes);

    // set the data into the realtime input map before serializing
    // TODO can't I just cache the last little map and set it there at least?
    _realtimeImageInputMap['realtimeInput']['mediaChunks'][0]['data'] = base64image;

    // send data to websocket
    _log.info('sending photo');
    _channel!.sink.add(jsonEncode(_realtimeImageInputMap));
  }

  /// If there is any audio that has been received from Gemini, ready for playback
  bool hasResponseAudio() {
    return _audioBuffer.isNotEmpty;
  }

  /// Returns PCM16 24kHz samples as ByteData (interpret as PCM16)
  ByteData getResponseAudioByteData() {
    if (hasResponseAudio()) {
      return (_audioBuffer.removeFirst()).buffer.asByteData();
    }
    else {
      return ByteData(0);
    }
  }

  /// Clears the audio buffer so the main app can't pull any more samples
  void stopResponseAudio() {
    // by clearing the buffered PCM data, the player will stop being fed audio
    _audioBuffer.clear();
  }

  /// handle the Gemini server events that come through the websocket
  /// TODO work out how the closed/session time is up message comes back - maybe just the socket status subscription?
  FutureOr<void> _handleGeminiEvent(dynamic eventJson) async {
    String eventString = utf8.decode(eventJson);

    // parse the json
    var event = jsonDecode(eventString);

    // try audio message types first
    var audioData = AudioDataExtractor.extractAudioData(event);

    if (audioData != null) {
      for (var chunk in audioData) {
        _audioBuffer.add(chunk);

        // notify the main app in case playback had stopped, it should start again
        audioReadyCallback();
      }
    }
    else {
      // some other kind of event
      var serverContent = event['serverContent'];
      if (serverContent != null) {
        if (serverContent['interrupted'] != null) {
          // TODO work out how much audio had already been played/how much was unplayed

          // process interruption to stop audio
          _audioBuffer.clear();
          eventLogger('---Interruption---');
          _log.fine('Response interrupted by user');

          // TODO communicate interruption playback point back to server?
        }
        else if (serverContent['turnComplete'] != null) {
          // server has finished sending
          eventLogger('Server turn complete');
        }
        else {
          eventLogger(serverContent.toString());
        }
      }
      else if (event['setupComplete'] != null) {
        eventLogger('Setup is complete');
        _log.info('Gemini setup is complete');
      }
      else {
        // unknown server message
        _log.info(eventString);
        eventLogger(eventString);
      }
    }
  }

}