module DataNodeProxy where

import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.IO
import Data.Binary(encode,decode)
import Control.Monad(forever)

import Messages

datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = forever $ do
  (h,_,_) <- liftIO $ accept socket
  liftIO $ hSetBuffering h NoBuffering
  liftIO $ hSetBinaryMode h True
  say "received connection"
  handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  say $ "wait for message"
  msg <- liftIO $ L.hGetContents h
  say $ "received msg"

  handleMessage (fromByteString msg) h pid

{-getData :: Handle -> Process L.ByteString
getData h = chunks B.empty
  where
    chunks xs = do
      eof <- liftIO $ hIsEOF h
      if eof
        then do say "handle is empty"; return xs
        else do
          say "handle not empty yet"
          chnk <- liftIO $ L.hGetLine h
          chunks (xs `L.append` chnk)-}

handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  say "received read"
  (sendport, receiveport) <- newChan
  send pid (CDNReadP bid sendport)
  resp <- receiveChan receiveport
  say $ show resp
  liftIO $ L.hPutStrLn h $ toByteString $ FileBlock resp
  say $ "send fileblock"

handleMessage (CDNWrite bid fd) h pid = do
  say $ "received write request"
  send pid (CDNWriteP bid fd)

handleMessage (CDNDelete bid) h pid = do
  say "received delete"
  send pid (CDNDeleteP bid)
