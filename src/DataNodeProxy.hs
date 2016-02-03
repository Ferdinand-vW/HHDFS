module DataNodeProxy where
{-}
import Network
import Control.Distributed.Process hiding (handleMessage)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.IO
import Data.Binary (encode,decode)
import Control.Monad (forever,unless)

import Messages

dnDataDir = "./data/"

datanodeproxy :: Socket -> ProcessId -> Process ()
datanodeproxy socket pid = forever $ do
  (h,_,_) <- liftIO $ accept socket
  spawnLocal $ handleClient h pid

handleClient :: Handle -> ProcessId -> Process ()
handleClient h pid = do
  liftIO $ hSetBuffering h NoBuffering
  --liftIO $ hSetBinaryMode h No
  msg <- liftIO $ B.hGetLine h
  say $ show (fromByteString msg :: ClientToDataNode)
  handleMessage (fromByteString msg) h pid

handleMessage :: ClientToDataNode -> Handle -> ProcessId -> Process ()
handleMessage (CDNRead bid) h pid = do
  file <- liftIO $ B.readFile (getFileName bid)
  liftIO $ B.hPut h $ file
  say $ "sent file to client"
  liftIO $ hClose h
  say $ "close handle"

handleMessage (CDNWrite bid) h pid = do
  unless (bid /= 10) $ say $ "received blockid " ++ show bid
  unless (bid == 10) $ say $ "received other blockid " ++ show bid
  fd <- liftIO $ B.hGetContents h
  say $ "read from handle"
  liftIO $ B.writeFile (getFileName bid) fd
  say $ "written to file"
  send pid (CDNWriteP bid)
  say $ "send to datanode"


handleMessage (CDNDelete bid) h pid = do
  say "received delete"
  send pid (CDNDeleteP bid)

getFileName :: BlockId -> FilePath
getFileName bid = dnDataDir ++ show bid ++ ".dat"
-}