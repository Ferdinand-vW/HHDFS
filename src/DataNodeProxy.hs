module DataNodeProxy where

import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.IO
import Data.Binary (encode,decode)
import Control.Monad (forever)

import Messages

dnDataDir :: String
dnDataDir = "./data/"

-- We wait for a client to connect with a socket. We then spawn a local
-- thread to handle the clients requests.
datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = forever $ do
  (h,_,_) <- liftIO $ accept socket
  spawnLocal $ handleClient h pid

-- Once the client has connected we simply wait for a message on the socket
handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  liftIO $ hSetBuffering h NoBuffering
  liftIO $ hSetBinaryMode h True
  msg <- liftIO $ L.hGetContents h

  handleMessage (fromByteString msg) h pid
  liftIO $ hClose h

-- If the client asks to read a file the proxy will return it directly to him
handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  liftIO $ putStrLn $ "Received read for " ++ show bid
  file <- liftIO $ L.readFile (getFileName bid)
  liftIO $ L.hPutStrLn h $ toByteString $ FileBlock file

-- If the client wants to read a file we write it locally and send a CDNWrite to
-- the datanode to record the new file
handleMessage (CDNWrite bid fd) h pid = do
  liftIO $ putStrLn $ "Received write of " ++ show bid
  liftIO $ B.writeFile (getFileName bid) (L.toStrict fd)
  send pid (CDNWriteP bid)


getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
