import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:meridian_example/main.dart';

// The example app opens a WebSocket to the (absent) signaling server as
// soon as it mounts, so this widget test only pins what is deterministic
// here: the app shell builds. Live overlay behavior is exercised by the
// package suite and real-browser runs, not this harness.
void main() {
  testWidgets('the example app shell builds', (WidgetTester tester) async {
    await tester.pumpWidget(const MeridianExampleApp());
    expect(find.byType(MaterialApp), findsOneWidget);
  });
}
