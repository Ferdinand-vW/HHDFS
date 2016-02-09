module NameNodeProxy where

import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.IO
import Data.Binary(encode,decode)
import Control.Monad(forever)
import qualified Network.Socket as S

import Messages

-- We wait for a client to connect with a socket. We then spawn a local
-- thread to handle the clients requests.
namenodeproxy :: Socket -> ProcessId -> Process ()
namenodeproxy socket pid = do
  --liftIO $ S.setSocketOption socket S.RecvTimeOut 0
  liftIO $ S.setSocketOption socket S.ReuseAddr 1
  forever $ do
    (h,_,_) <- liftIO $ accept socket
    spawnLocal $ handleClient h pid

  -- Once the client has connected we simply wait for a message on the socket
handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  liftIO $ hSetBuffering h NoBuffering
  msg <- liftIO $ B.hGetLine h
  handleMessage (fromByteString msg) h pid
  -- close the handle once the message is handled
  --liftIO $ hClose h

-- We simply forward every message to the Datanode process and wait for the
-- response on a channel. Once we recieve the response we send it back to
-- the client via the socket.
handleMessage :: ClientToNameNode -> Handle -> ProcessId -> Process ()
handleMessage ListFiles h pid  = do
  (sendport,receiveport) <- newChan
  send pid (ListFilesP sendport)
  resp <- receiveChan receiveport
  liftIO $ B.hPutStrLn h $ toByteString  $ FilePaths resp

handleMessage (Read fp) h pid = do
  (sendport,receiveport) <- newChan
  liftIO $ putStrLn "send read to namenode"
  send pid (ReadP fp sendport)
  resp <- receiveChan receiveport
  liftIO $ putStrLn "send read addresses"
  liftIO $ B.hPutStrLn h $ toByteString $ ReadAddress resp
  liftIO $ putStrLn $ show $ toByteString $ ReadAddress resp
  liftIO $ putStrLn "done sending"

handleMessage (Write fp bc) h pid = do
  (sendport,receiveport) <- newChan
  send pid (WriteP fp bc sendport)
  resp <- receiveChan receiveport
  liftIO $ putStrLn "send write addresses"
  liftIO $ B.hPutStrLn h $ toByteString $ WriteAddress resp
  liftIO $ putStrLn $ show $ toByteString $ WriteAddress resp
  liftIO $ putStrLn "done sending"
