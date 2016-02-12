#!/bin/sh
wd=$(pwd)
osascript <<-eof
tell application "iTerm"
  set myterm to (make new terminal)

  tell myterm
    launch session "Default session"
    tell the last session
      set name to "TestClient"
      write text "cd \"$wd\"/TestClient"
      write text "./hhdfs test 127.0.0.1 44445q"
    end tell
  end tell
end tell
eof
