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
  spawnLocal $ handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  liftIO $ hSetBuffering h NoBuffering
  msg <- liftIO $ L.hGetContents h
  handleMessage (decode msg) h pid
  liftIO $ hClose h

handleMessage :: ClientToNameNode -> Handle -> ProcessId -> Process ()
handleMessage ListFiles h pid  = do
  (sendport,receiveport) <- newChan
  send pid (ListFilesP sendport)
  resp <- receiveChan receiveport
  liftIO $ L.hPutStrLn h $ encode  $ FilePaths resp

handleMessage (Read fp) h pid = do
  (sendport,receiveport) <- newChan
  send pid (ReadP fp sendport)
  resp <- receiveChan receiveport
  liftIO $ L.hPutStrLn h $ encode $ ReadAddress resp

handleMessage (Write fp bc) h pid = do
  (sendport,receiveport) <- newChan
  send pid (WriteP fp bc sendport)
  resp <- receiveChan receiveport
  liftIO $ L.hPutStrLn h $ toByteString $ WriteAddress resp