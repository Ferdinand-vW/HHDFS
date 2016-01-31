module Proxy where

import           Control.Distributed.Process
import           Network.Socket hiding(sendAll,recv)
import           Network.Socket.ByteString
import           Data.Binary (encode)
import qualified Data.ByteString.Lazy.Char8 as L
import           Control.Monad (forever)

import           Messages

proxy :: ProcessId -> Socket -> IO ()
proxy pid sock = 
  forever $ do
    (conn,_) <- accept sock
    sendAll conn (L.toStrict $ encode $ Response pid)
    close conn
