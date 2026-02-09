import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter/material.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart' as fbp;
import 'package:flutter_foreground_task/flutter_foreground_task.dart';
import 'package:flutter_pcm_sound/flutter_pcm_sound.dart';
import 'package:frame_realtime_gemini_voicevision/audio_upsampler.dart';
import 'package:frame_realtime_gemini_voicevision/gemini_realtime.dart';
import 'package:logging/logging.dart';
import 'package:share_plus/share_plus.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:frame_msg/rx/audio.dart';
import 'package:frame_msg/rx/photo.dart';
import 'package:frame_msg/rx/tap.dart';
import 'package:frame_msg/tx/capture_settings.dart';
import 'package:frame_msg/tx/code.dart';
import 'package:frame_msg/tx/plain_text.dart';
import 'package:simple_frame_app/simple_frame_app.dart';
import 'foreground_service.dart';

void main() {
  // Set up Android foreground service
  if (Platform.isAndroid) {
    initializeForegroundService();
  }

  // quieten FBP logs
  fbp.FlutterBluePlus.setLogLevel(fbp.LogLevel.info);

  runApp(const MainApp());
}

final _log = Logger("MainApp");

class MainApp extends StatefulWidget {
  const MainApp({super.key});

  @override
  MainAppState createState() => MainAppState();
}

/// SimpleFrameAppState mixin helps to manage the lifecycle of the Frame connection outside of this file
class MainAppState extends State<MainApp> with SimpleFrameAppState {

  /// realtime voice application members
  late final GeminiRealtime _gemini;
  GeminiVoiceName _voiceName = GeminiVoiceName.Puck;

  // status of audio output with FlutterPCMSound
  bool _playingAudio = false;

  // true when audio/photos are being streamed from Frame
  bool _streaming = false;

  // tap subscription
  StreamSubscription<int>? _tapSubs;

  // Audio: 8kHz 16-bit linear PCM from Frame mic (only the high 10 bits iirc)
  final RxAudio _rxAudio = RxAudio(streaming: true);
  StreamSubscription<Uint8List>? _frameAudioSubs;
  Stream<Uint8List>? _frameAudioSampleStream;

  // Photos: 720px VERY_HIGH quality JPEGs
  static const resolution = 720;
  static const qualityIndex = 4;
  static const qualityLevel = 'VERY_HIGH';
  final RxPhoto _rxPhoto = RxPhoto(quality: qualityLevel, resolution: resolution);
  StreamSubscription<Uint8List>? _photoSubs;
  Stream<Uint8List>? _photoStream;
  static const int photoInterval = 3;
  Timer? _photoTimer;
  Image? _image;

  // UI display
  final _apiKeyController = TextEditingController();
  final _systemInstructionController = TextEditingController();
  final List<String> _eventLog = List.empty(growable: true);
  final _eventLogController = ScrollController();
  static const _textStyle = TextStyle(fontSize: 20);
  String? _errorMsg;

  MainAppState() {
    // filter logging
    hierarchicalLoggingEnabled = true;
    Logger.root.level = Level.FINE;
    Logger('Bluetooth').level = Level.FINE;
    Logger('RxPhoto').level = Level.FINE;
    Logger('RxAudio').level = Level.FINE;
    Logger('RxTap').level = Level.FINE;

    Logger.root.onRecord.listen((record) {
      debugPrint('${record.level.name}: [${record.loggerName}] ${record.time}: ${record.message}');
    });

    // Pass the "audio ready" and UI logger callbacks to GeminiRealtime class
    // so audio will play and events can be displayed
    _gemini = GeminiRealtime(_audioReadyCallback, _appendEvent);
  }

  @override
  void initState() {
    super.initState();

    _asyncInit();
  }

  Future<void> _asyncInit() async {
    // load up the saved text field data
    await _loadPrefs();

    // Initialize the audio playback framework
    // (Gemini sends response audio as mono pcm16 24kHz)
    const sampleRate = 24000;
    FlutterPcmSound.setLogLevel(LogLevel.error);
    await FlutterPcmSound.setup(sampleRate: sampleRate, channelCount: 1);
    FlutterPcmSound.setFeedThreshold(sampleRate ~/ 30);
    FlutterPcmSound.setFeedCallback(_onFeed);

    // then kick off the connection to Frame and start the app if possible, unawaited
    tryScanAndConnectAndStart(andRun: true);
  }

  /// Feed the audio player with samples if we have some more, but don't send
  /// too much to the player because we want to be able to interrupt quickly
  /// If we don't feed the player and it stops, we won't get called again so we need to kick it off again
  void _onFeed(int remainingFrames) async {
    if (remainingFrames < 2000) {
      if (_gemini.hasResponseAudio()) {
        await FlutterPcmSound.feed(PcmArrayInt16(bytes: _gemini.getResponseAudioByteData()));
      }
      else {
        _log.fine('Response audio ended');
        _playingAudio = false;
      }
    }
  }

  @override
  Future<void> dispose() async {
    await _gemini.disconnect();
    await _frameAudioSubs?.cancel();
    await FlutterPcmSound.release();
    _photoTimer?.cancel();
    super.dispose();
  }

  Future<void> _loadPrefs() async {
    final prefs = await SharedPreferences.getInstance();
    setState(() {
      _apiKeyController.text = prefs.getString('api_key') ?? '';
      _systemInstructionController.text = prefs.getString('system_instruction') ?? 'The stream of images are coming live from the user\'s smart glasses, they are not a recorded video. For example, don\'t say "the person in the video", say "the person in front of you" if you are referring to someone you can see in the images. If an image is blurry, don\'t say the image is too blurry, wait for subsequent images that will arrive in the coming few seconds that might stabilize focus and be easier to process.\n\nAfter the user asks a question, never restate the question but instead directly answer it. No need to start responding when the images come in, wait for the user to start talking and only refer to the live images when relevant.\n\nTry not to repeat what the user is asking unless you\'re really unsure.';
      _voiceName = GeminiVoiceName.values.firstWhere(
        (e) => e.toString().split('.').last == (prefs.getString('voice_name') ?? 'Puck'),
        orElse: () => GeminiVoiceName.Puck,
      );
    });
  }

  Future<void> _savePrefs() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString('api_key', _apiKeyController.text);
    await prefs.setString('system_instruction', _systemInstructionController.text);
    await prefs.setString('voice_name', _voiceName.name);
  }

  /// This application uses Gemini's realtime API over WebSockets.
  /// It has a running main loop in this function and also on the Frame (frame_app.lua)
  @override
  Future<void> run() async {
    // validate API key exists at least
    _errorMsg = null;
    if (_apiKeyController.text.isEmpty) {
      setState(() {
        _errorMsg = 'Error: Set value for Gemini API Key';
      });

      return;
    }

    // connect to Gemini realtime
    await _gemini.connect(_apiKeyController.text, _voiceName, _systemInstructionController.text);

    if (!_gemini.isConnected()) {
      _log.severe('Connection to Gemini failed');
      return;
    }

    setState(() {
      currentState = ApplicationState.running;
    });

    try {
      // listen for double taps to start/stop transcribing
      _tapSubs?.cancel();
      _tapSubs = RxTap().attach(frame!.dataResponse)
        .listen((taps) async {
          _log.info('taps: $taps');
          if (_gemini.isConnected()) {

            if (taps >= 2) {

              if (!_streaming) {
                await _startFrameStreaming();

                // show microphone emoji
                await frame!.sendMessage(0x0b, TxPlainText(text: '\u{F0010}').pack());
              }
              else {
                await _stopFrameStreaming();

                // prompt the user to begin tapping
                await frame!.sendMessage(0x0b, TxPlainText(text: 'Double-Tap to resume!').pack());
              }
            }
            // ignore spurious 1-taps
          }
          else {
            // Disconnected from Gemini, go back to Ready state
            _appendEvent('Disconnected from Gemini');

            _stopFrameStreaming();

            setState(() {
              currentState = ApplicationState.ready;
            });
          }
        });

      // let Frame know to subscribe for taps and send them to us
      await frame!.sendMessage(0x10, TxCode(value: 1).pack());

      // prompt the user to begin tapping
      await frame!.sendMessage(0x0b, TxPlainText(text: 'Double-Tap to begin!').pack());

    } catch (e) {
      _errorMsg = 'Error executing application logic: $e';
      _log.fine(_errorMsg);

      setState(() {
        currentState = ApplicationState.ready;
      });
    }
  }

  /// Once running(), audio streaming is controlled by taps. But the user can cancel
  /// here as well, whether they are currently streaming audio or not.
  @override
  Future<void> cancel() async {
    setState(() {
      currentState = ApplicationState.canceling;
    });

    // cancel the subscription for taps
    _tapSubs?.cancel();

    // cancel the conversation if it's running
    if (_streaming) _stopFrameStreaming();

    // tell the Frame to stop streaming audio (regardless of if we are currently)
    await frame!.sendMessage(0x30, TxCode(value: 0).pack());

    // let Frame know to stop sending taps too
    await frame!.sendMessage(0x10, TxCode(value: 0).pack());

    // clear the display
    await frame!.sendMessage(0x0b, TxPlainText(text: ' ').pack());

    // disconnect from Gemini
    await _gemini.disconnect();

    setState(() {
      currentState = ApplicationState.ready;
    });
  }

  /// When we receive a tap to start the conversation, we need to start
  /// audio and photo streaming on Frame
  Future<void> _startFrameStreaming() async {
    _appendEvent('Starting Frame Streaming');

    FlutterPcmSound.start();

    // app state is conversing; Gemini is connected for the entire duration
    _streaming = true;

    try {
      // the audio stream from Frame, which needs to be closed() to stop the streaming
      _frameAudioSampleStream = _rxAudio.attach(frame!.dataResponse);
      _frameAudioSubs?.cancel();
      // TODO consider buffering if 128 bytes of PCM16 / 64 bytes of ulaw is too little (e.g. if measured in requests not tokens)
      _frameAudioSubs = _frameAudioSampleStream!.listen(_handleFrameAudio);

      // tell Frame to start streaming audio
      await frame!.sendMessage(0x30, TxCode(value: 1).pack());
      // TODO why isn't _streaming = true set here?

      // immediately request a photo, then every few seconds while the conversation is running
      await _requestPhoto();
      _photoTimer = Timer.periodic(const Duration(seconds: photoInterval), (timer) async {
        _log.info('Timer Fired!');

        if (!_streaming) {
          timer.cancel();
          _photoTimer = null;
          _log.info('Streaming ended, stop requesting photos');
          return;
        }

        // send the request to Frame
        await _requestPhoto();
      });

    } catch (e) {
      _log.warning(() => 'Error executing application logic: $e');
    }
  }

  /// When we receive a tap to stop the conversation, cancel the audio streaming from Frame,
  /// which will send "final chunk" message, which will close the audio stream
  /// and the Gemini conversation needs to stop too
  Future<void> _stopFrameStreaming() async {
    _streaming = false;

    // stop audio playback
    // by clearing the buffered PCM data, the player will stop being fed audio
    _gemini.stopResponseAudio();

    // stop requesting photos periodically
    _photoTimer?.cancel();
    _photoTimer = null;

    // tell Frame to stop streaming audio
    await frame!.sendMessage(0x30, TxCode(value: 0).pack());

    // rxAudio.detach() to close/flush the controller controlling our audio stream
    _rxAudio.detach();

    _appendEvent('Ending Frame Streaming');
  }

  /// Request a photo from Frame
  Future<void> _requestPhoto() async {
    _log.info('requesting photo from Frame');

    // prepare to receive the photo from Frame
    // this must happen each time as the stream
    // closes after each photo is sent
    _photoStream = _rxPhoto.attach(frame!.dataResponse);
    _photoSubs?.cancel();
    _photoSubs = _photoStream!.listen(_handleFramePhoto);

    // TODO check if we can request a raw (headerless) jpeg
    //_rxPhoto.

    await frame!.sendMessage(0x0d, TxCaptureSettings(resolution: resolution, qualityIndex: qualityIndex).pack());
  }


  /// pass the audio from Frame (upsampled) to the API
  void _handleFrameAudio(Uint8List pcm16x8) {
    if (_gemini.isConnected()) {
      // upsample PCM16 from 8kHz to 16kHz for Gemini
      var pcm16x16 = AudioUpsampler.upsample8kTo16k(pcm16x8);

      // send audio up to Gemini
      _gemini.sendAudio(pcm16x16);
    }
  }

    /// pass the photo from Frame to the API
  void _handleFramePhoto(Uint8List jpegBytes) {
    _log.info('photo received from Frame');
    if (_gemini.isConnected()) {

      _gemini.sendPhoto(jpegBytes);
    }

    // update the UI with the latest image
    setState(() {
      _image = Image.memory(jpegBytes, gaplessPlayback: true);
    });
  }

  /// Notification from GeminiRealtime that some audio is ready for playback
  void _audioReadyCallback() {
    // kick off playback if it's not playing
    if (!_playingAudio) {
      _playingAudio = true;
      _onFeed(0);
      _log.fine('Response audio started');
    }
  }

  /// puts some text into our scrolling log in the UI
  void _appendEvent(String evt) {
    setState(() {
      _eventLog.add(evt);
    });
    _scrollToBottom();
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_eventLogController.hasClients) {
        _eventLogController.animateTo(
          _eventLogController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 300),
          curve: Curves.easeOut,
        );
      }
    });
  }

  @override
  Widget build(BuildContext context) {
    startForegroundService();
    return WithForegroundTask(
      child: MaterialApp(
        title: 'Frame Realtime Gemini Voice and Vision',
        theme: ThemeData.dark(),
        home: Scaffold(
          appBar: AppBar(
            title: const Text('Frame Realtime Gemini Voice and Vision'),
            actions: [getBatteryWidget()]
          ),
          body: Center(
            child: Container(
              margin: const EdgeInsets.symmetric(horizontal: 16),
              child: Column(
                mainAxisAlignment: MainAxisAlignment.start,
                crossAxisAlignment: CrossAxisAlignment.end,
                children: [
                  Row(
                    children: [
                      Expanded(
                        child: TextField(
                          controller: _apiKeyController,
                          decoration: const InputDecoration(hintText: 'Enter Gemini API Key'),
                        ),
                      ),
                      const SizedBox(width: 10),
                      DropdownButton<GeminiVoiceName>(
                        value: _voiceName,
                        onChanged: (GeminiVoiceName? newValue) {
                          setState(() {
                          _voiceName = newValue!;
                          });
                        },
                        items: GeminiVoiceName.values.map<DropdownMenuItem<GeminiVoiceName>>((GeminiVoiceName value) {
                          return DropdownMenuItem<GeminiVoiceName>(
                            value: value,
                            child: Text(value.toString().split('.').last),
                          );
                        }).toList(),
                      ),
                    ],
                  ),
                  const SizedBox(height: 10),
                  TextField(
                    controller: _systemInstructionController,
                    maxLines: 3,
                    decoration: const InputDecoration(hintText: 'System Instruction'),
                  ),
                  if (_errorMsg != null) Text(_errorMsg!, style: const TextStyle(backgroundColor: Colors.red)),
                  ElevatedButton(onPressed: _savePrefs, child: const Text('Save')),

                  Expanded(child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      _image ?? Container(),
                      Expanded(
                        child: ListView.builder(
                          controller: _eventLogController, // Auto-scroll controller
                          itemCount: _eventLog.length,
                          itemBuilder: (context, index) {
                            return Text(
                              _eventLog[index],
                              style: _textStyle,
                            );
                          },
                        ),
                      ),
                    ],
                  )),
                ],
              ),
            ),
          ),
          floatingActionButton: Stack(
            children: [
              if (_eventLog.isNotEmpty) Positioned(
                bottom: 90,
                right: 20,
                child: FloatingActionButton(
                  onPressed: () {
                    Share.share(_eventLog.join('\n'));
                  },
                  child: const Icon(Icons.share)),
              ),
              Positioned(
                bottom: 20,
                right: 20,
                child: getFloatingActionButtonWidget(const Icon(Icons.mic), const Icon(Icons.mic_off)) ?? Container(),
              ),
            ]
          ),
          persistentFooterButtons: getFooterButtonsWidget(),
        ),
      )
    );
  }
}
