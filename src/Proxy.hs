module Proxy where

import           Control.Distributed.Process
import           Network.Socket hiding(sendAll,recv)
import           Network.Socket.ByteString
import           Data.Binary (encode)
import qualified Data.ByteString.Lazy.Char8 as L
import           Control.Monad (forever)

import           Messages

{-proxy :: ProcessId -> Socket -> IO ()
proxy pid sock = 
  loop 2
 where 
    loop 0 = close sock
    loop n = do
        (conn,_) <- accept sock
        putStrLn "Received Connection"
        sendAll conn (L.toStrict $ encode $ Response pid)
        close conn
        loop (n - 1)-}