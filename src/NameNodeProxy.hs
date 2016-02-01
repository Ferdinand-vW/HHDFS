module NameNodeProxy where

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
  handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = forever $ do
  msg <- liftIO $ B.hGetLine h
  handleMessage (decode $ L.fromStrict msg) h pid

handleMessage :: ClientToNameNode -> Handle -> ProcessId -> Process ()
handleMessage ListFiles h pid  = do
  (sendport,receiveport) <- newChan
  send pid (ListFilesP sendport)
  resp <- receiveChan receiveport
  liftIO $ B.hPutStrLn h $ L.toStrict $ encode  $ FilePaths resp

handleMessage (Read fp) h pid = do
  (sendport,receiveport) <- newChan
  send pid (ReadP fp sendport)
  resp <- receiveChan receiveport
  say $ show resp
  liftIO $ B.hPutStrLn h $ L.toStrict $ encode $ ReadAddress resp

handleMessage (Write fp bc) h pid = do
  (sendport,receiveport) <- newChan
  send pid (WriteP fp bc sendport)
  resp <- receiveChan receiveport
  say $ show resp
  liftIO $ B.hPutStrLn h $ toByteString $ WriteAddress resp