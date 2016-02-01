module DataNodeProxy where

import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import System.IO
import Data.Binary(encode,decode)
import Control.Monad(forever)

import Messages

datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = forever $ do
  (h,_,_) <- liftIO $ accept socket
  say "received connection"
  handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  msg <- liftIO $ B.hGetLine h
  say $ "received msg"
  say $ show msg
  let CDNWrite bid fdata = fromByteString msg
  say $ show bid
  handleMessage (fromByteString msg) h pid
  liftIO $ hClose h

handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  say "received read"
  (sendport, receiveport) <- newChan
  send pid (CDNReadP bid sendport)
  resp <- receiveChan receiveport
  liftIO $ B.hPutStrLn h $ toByteString $ FileBlock resp

handleMessage (CDNWrite bid fd) h pid = do
  say $ "received write request"
  send pid (CDNWriteP bid fd)

handleMessage (CDNDelete bid) h pid = do
  say "received delete"
  send pid (CDNDeleteP bid)