module Relay where

import Network.Socket
import System.Random

relay :: [SockAddr] -> Socket -> IO ()
relay proxies sock = do
    forever $ do
      (conn,_) <- accept sock
      putStrLn "Received Connection"
      n <- randomRIO(0,length proxies - 1) :: IO Int
      sendAll conn (L.toStrict $ encode $ proxies ! n)
      close conn
