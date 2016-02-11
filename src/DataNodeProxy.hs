module DataNodeProxy where

import Network.Socket hiding (send)
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import qualified System.IO as IO
import Data.Binary (encode,decode)
import Control.Monad (forever)
import qualified Network.Socket as S
import System.IO.Streams (InputStream, OutputStream)
import qualified System.IO.Streams as Streams

import Messages

dnDataDir :: String
dnDataDir = "./data/"

-- We wait for a client to connect with a socket. We then spawn a local
-- thread to handle the clients requests.
datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = do
  forever $ do
    (sock,_) <- liftIO $ accept socket
    liftIO $ putStrLn "Accepted connection"
    spawnLocal $ do
      handleClient sock pid
      liftIO $ sClose sock

-- Once the client has connected we simply wait for a message on the socket
handleClient :: Socket -> ProcessId -> Process ()
handleClient sock pid = do
  (prod,cons) <- liftIO $ Streams.socketToStreams sock
  Just msg <- liftIO $ Streams.read prod

  handleMessage (fromByteString msg) (prod,cons) pid

-- If the client asks to read a file the proxy will return it directly to him
handleMessage :: ClientToDataNode -> (InputStream B.ByteString,OutputStream B.ByteString) -> ProcessId -> Process ()
handleMessage (CDNRead bid) (prod,cons) pid = liftIO $ do
  putStrLn $ "Received read for " ++ show bid
  Streams.withFileAsInput (getFileName bid) (\fprod -> do
    Streams.connect fprod cons)

-- If the client wants to read a file we write it locally and send a CDNWrite to
-- the datanode to record the new file
handleMessage (CDNWrite bid) (prod,cons) pid = do
  liftIO $ do
    Streams.write (Just $ toByteString OK) cons
    putStrLn $ "Received write of " ++ show bid
    Streams.withFileAsOutput (getFileName bid) (\fcons -> do
      Streams.connect prod fcons
      )

  send pid (CDNWriteP bid)



getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
