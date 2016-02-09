module DataNodeProxy where

import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import qualified System.IO as IO
import Data.Binary (encode,decode)
import Control.Monad (forever)
import qualified Network.Socket as S
import qualified Pipes.ByteString as PB
import qualified Pipes as P

import Messages

dnDataDir :: String
dnDataDir = "./data/"

-- We wait for a client to connect with a socket. We then spawn a local
-- thread to handle the clients requests.
datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = do
  --liftIO $ S.setSocketOption socket S.RecvTimeOut 0
  liftIO $ S.setSocketOption socket S.ReuseAddr 1
  forever $ do
    (h,_,_) <- liftIO $ accept socket
    liftIO $ putStrLn "Accepted connection"
    spawnLocal $ handleClient h pid

-- Once the client has connected we simply wait for a message on the socket
handleClient :: IO.Handle -> ProcessId -> Process ()
handleClient h pid = do
  liftIO $ IO.hSetBuffering h IO.NoBuffering
  liftIO $ IO.hSetBinaryMode h True
  msg <- liftIO $ B.hGetLine h
  liftIO $ putStrLn "received msg"

  handleMessage (decode $ L.fromStrict msg) h pid
  liftIO $ IO.hClose h

-- If the client asks to read a file the proxy will return it directly to him
handleMessage :: ClientToDataNode -> IO.Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  liftIO $ putStrLn $ "Received read for " ++ show bid
  --file <- liftIO $ L.readFile (getFileName bid)
  --liftIO $ L.hPutStrLn h $ encode $ FileBlock file
  fhandle <- liftIO $ IO.openFile (getFileName bid) IO.ReadMode
  let fprod = PB.fromHandle fhandle
      fcons = PB.toHandle h
  liftIO $ P.runEffect $ fprod P.>-> fcons
  liftIO $ IO.hClose fhandle

-- If the client wants to read a file we write it locally and send a CDNWrite to
-- the datanode to record the new file
handleMessage (CDNWrite bid) h pid = do
  liftIO $ putStrLn $ "Received write of " ++ show bid
  let fprod = PB.fromHandle h
  fhandle <- liftIO $ IO.openFile (getFileName bid) IO.WriteMode
  let fcons = PB.toHandle fhandle
  liftIO $ P.runEffect $ fprod P.>-> fcons
  liftIO $ IO.hClose fhandle
  --liftIO $ B.writeFile (getFileName bid) (L.toStrict fd)
  send pid (CDNWriteP bid)


getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
