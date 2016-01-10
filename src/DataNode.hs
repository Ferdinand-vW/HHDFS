{-# LANGUAGE TemplateHaskell #-}

module DataNode where

import Control.Distributed.Process
import Control.Distributed.Process.Closure

import Messages (HandShake(..))

dataNode :: ProcessId -> Process ()
dataNode nnid = do
  -- Send WhereIsReply with the ProcessId of the data node
  pid <- getSelfPid
  send nnid $ WhereIsReply "" (Just pid)

  -- Create a channel, and send the sendPort to the data node
  (sendPort, receivePort) <- newChan
  send nnid $ HandShake sendPort

  -- TODO keep receiving:
    -- store block id
    -- write data to blockid
    -- delete block from folder and map
    -- send data to sendport?

  return ()

remotable ['dataNode]
