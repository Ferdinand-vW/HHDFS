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
  msg <- liftIO $ B.hGetLine h
  say $ "received msg"

  handleMessage (fromByteString msg) h pid

handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  say "received read"
  file <- liftIO $ B.readFile (getFileName bid)
  liftIO $ B.hPut h $ toByteString $ FileBlock file
  liftIO $ hClose h
  say $ "send fileblock"

handleMessage (CDNWrite bid) h pid = do
  say $ "received write request"
  fd <- liftIO $ B.hGetContents h
  liftIO $ B.writeFile (getFileName bid) fd
  send pid (CDNWriteP bid)


handleMessage (CDNDelete bid) h pid = do
  say "received delete"
  send pid (CDNDeleteP bid)

getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
