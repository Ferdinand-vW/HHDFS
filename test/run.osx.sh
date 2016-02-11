#!/bin/sh
wd=$(pwd)
osascript <<-eof
tell application "iTerm"
  set myterm to (make new terminal)

  tell myterm
    launch session "Default session"
    tell the last session
      set name to "NameNode"
      write text "cd \"$wd\"/NameNode"
      write text "./hhdfs namenode 127.0.0.1 44444"
    end tell

    launch session "Default session"
    tell the last session
      set name to "DataNode#1"
      write text "cd \"$wd\"/DataNode#1"
      write text "./hhdfs datanode 127.0.0.1 44446 127.0.0.1:44444:0"
    end tell

    launch session "Default session"
    tell the last session
      set name to "DataNode#2"
      write text "cd \"$wd\"/DataNode#2"
      write text "./hhdfs datanode 127.0.0.1 44448 127.0.0.1:44444:0"
    end tell

    # launch session "Default session"
    # tell the last session
    #   set name to "DataNode#3"
    #   write text "cd \"$wd\"/DataNode#3"
    #   write text "./hhdfs datanode 127.0.0.1 44450 127.0.0.1:44444:0"
    # end tell
    #
    # launch session "Default session"
    # tell the last session
    #   set name to "DataNode#4"
    #   write text "cd \"$wd\"/DataNode#4"
    #   write text "./hhdfs datanode 127.0.0.1 44452 127.0.0.1:44444:0"
    # end tell

    launch session "Default session"
    tell the last session
      set name to "Client"
      write text "cd \"$wd\"/Client"
      write text "./hhdfs client 127.0.0.1 44445"
    end tell

  end tell
end tell
eof
