module DataNodeProxy where

import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.IO
import Control.Monad(forever)

import Messages

datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = forever $ do
  (h,_,_) <- liftIO $ accept socket
  liftIO $ hSetBuffering h NoBuffering
  say "received connection"
  handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  say $ "wait for message"
  msg <- liftIO $ B.hGetContents h
  say $ "received msg"
  say $ show msg
  handleMessage (fromByteString msg) h pid
  liftIO $ hClose h
  say $ "closed handle"
  open <- liftIO $ hIsOpen h
  say $ show open

handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  say "received read"
  (sendport, receiveport) <- newChan
  send pid (CDNReadP bid sendport)
  resp <- receiveChan receiveport
  say $ show resp
  liftIO $ B.hPutStrLn h $ toByteString $ FileBlock resp
  say $ "send fileblock"

handleMessage (CDNWrite bid fd) h pid = do
  say $ "received write request"
  send pid (CDNWriteP bid fd)

handleMessage (CDNDelete bid) h pid = do
  say "received delete"
  send pid (CDNDeleteP bid)