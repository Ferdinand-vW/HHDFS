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
  msg <- liftIO $ L.hGetContents h

  handleMessage (fromByteString msg) h pid
  liftIO $ hClose h

handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  liftIO $ putStrLn $ "Received read for " ++ show bid
  file <- liftIO $ L.readFile (getFileName bid)
  liftIO $ L.hPutStrLn h $ toByteString $ FileBlock file

handleMessage (CDNWrite bid fd) h pid = do
  liftIO $ putStrLn $ "Received write of " ++ show bid
  liftIO $ B.writeFile (getFileName bid) (L.toStrict fd)
  send pid (CDNWriteP bid)


handleMessage (CDNDelete bid) h pid = do
  say "received delete"
  send pid (CDNDeleteP bid)

getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
