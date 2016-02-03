module NameNodeProxy where
{-
import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.IO
import Data.Binary(encode,decode)
import Control.Monad(forever)

import Messages

namenodeproxy :: Socket -> ProcessId -> Process ()
namenodeproxy socket pid = forever $ do
  (h,_,_) <- liftIO $ accept socket
  spawnLocal $ handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  liftIO $ hSetBuffering h LineBuffering
  --liftIO $ hSetBinaryMode h True
  say $ "client connected"
  msg <- liftIO $ B.hGetLine h
  say $ show $ (fromByteString msg :: ClientToNameNode)
  say $ "received message"
  handleMessage (fromByteString msg) h pid
  liftIO $ hClose h

handleMessage :: ClientToNameNode -> Handle -> ProcessId -> Process ()
handleMessage ListFiles h pid  = do
  (sendport,receiveport) <- newChan
  send pid (ListFilesP sendport)
  resp <- receiveChan receiveport
  liftIO $ B.hPut h $ toByteString  $ FilePaths resp

handleMessage (Read fp) h pid = do
  (sendport,receiveport) <- newChan
  say $ "send message to namenode"
  send pid (ReadP fp sendport)
  say $ "sent message to namenode"
  resp <- receiveChan receiveport
  say $ "received response"
  say $ show resp
  liftIO $ B.hPut h $ toByteString $ ReadAddress resp

handleMessage (Write fp bc) h pid = do
  (sendport,receiveport) <- newChan
  send pid (WriteP fp bc sendport)
  resp <- receiveChan receiveport
  say $ show resp
  liftIO $ B.hPut h $ toByteString $ WriteAddress resp-}