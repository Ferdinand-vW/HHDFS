module DataNodeProxy where

import Network.Socket hiding (send)
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import qualified System.IO as IO
import Data.Binary (encode,decode)
import Control.Monad (forever)
import qualified Network.Socket as S
import qualified Pipes.ByteString as PB
import qualified Pipes as P
import System.IO.Streams (InputStream, OutputStream)
import qualified System.IO.Streams as Streams

import Messages

dnDataDir :: String
dnDataDir = "./data/"

-- We wait for a client to connect with a socket. We then spawn a local
-- thread to handle the clients requests.
datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = do
  --liftIO $ S.setSocketOption socket S.RecvTimeOut 0
  forever $ do
    (sock,_) <- liftIO $ accept socket
    liftIO $ putStrLn "Accepted connection"
    spawnLocal $ do
      handleClient sock pid
      liftIO $ sClose sock

-- Once the client has connected we simply wait for a message on the socket
handleClient :: Socket -> ProcessId -> Process ()
handleClient sock pid = do
  {-liftIO $ IO.hSetBuffering h IO.NoBuffering
  liftIO $ IO.hSetBinaryMode h True-}
  (prod,cons) <- liftIO $ Streams.socketToStreams sock
  Just msg <- liftIO $ Streams.read prod

  --liftIO $ putStrLn "received msg"

  handleMessage (fromByteString msg) (prod,cons) pid

-- If the client asks to read a file the proxy will return it directly to him
handleMessage :: ClientToDataNode -> (InputStream B.ByteString,OutputStream B.ByteString) -> ProcessId -> Process ()
handleMessage (CDNRead bid) (prod,cons) pid = do
  liftIO $ putStrLn $ "Received read for " ++ show bid
  --liftIO $ L.hPutStrLn h $ encode $ FileBlock file
  --fhandle <- liftIO $ IO.openFile (getFileName bid) IO.ReadMode
  --fprod <- liftIO $ Streams.handleToInputStream fhandle
  liftIO $ Streams.withFileAsInput (getFileName bid) (\fprod -> do
    Streams.connect fprod cons)
  --liftIO $ P.runEffect $ fprod P.>-> fcons

-- If the client wants to read a file we write it locally and send a CDNWrite to
-- the datanode to record the new file
handleMessage (CDNWrite bid) (prod,cons) pid = do
  liftIO $ Streams.write (Just $ toByteString OK) cons
  liftIO $ putStrLn $ "Received write of " ++ show bid
  --(prod,cons) <- liftIO $ Streams.handleToStreams h
  --fhandle <- liftIO $ IO.openFile (getFileName bid) IO.WriteMode
  liftIO $ Streams.withFileAsOutput (getFileName bid) (\fcons -> do
    Streams.connect prod fcons
    )
  --liftIO $ putStrLn "connect streams"
  --liftIO $ putStrLn "done writing"
  --liftIO $ putStrLn "completed connected streams"
  --liftIO $ B.writeFile (getFileName bid) (L.toStrict fd)
  --liftIO $ putStrLn "send write to datanode"
  liftIO $ putStrLn "Closing socket now"
  send pid (CDNWriteP bid)
  liftIO $ putStrLn "write is complete"
  --liftIO $ Streams.write (Just $ toByteString WriteComplete) cons
  --liftIO $ putStrLn "done"


getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
