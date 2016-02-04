module DataNodeProxy where

import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.IO
import Data.Binary (encode,decode)
import Control.Monad (forever)

import Messages

dnDataDir = "./data/"

datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = forever $ do
  (h,_,_) <- liftIO $ accept socket
  spawnLocal $ handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  liftIO $ hSetBuffering h NoBuffering
  liftIO $ hSetBinaryMode h True
  say "received connection"
  say $ "wait for message"
  msg <- liftIO $ L.hGetContents h
  say $ "received msg"

  handleMessage (fromByteString msg) h pid
  liftIO $ hClose h

handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  say "received read"
  file <- liftIO $ L.readFile (getFileName bid)
  liftIO $ L.hPutStrLn h $ toByteString $ FileBlock file
  say $ "send fileblock"

handleMessage (CDNWrite bid fd) h pid = do
  say $ "received write request"
  liftIO $ B.writeFile (getFileName bid) (L.toStrict fd)
  send pid (CDNWriteP bid)


handleMessage (CDNDelete bid) h pid = do
  say "received delete"
  send pid (CDNDeleteP bid)

getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
