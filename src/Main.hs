module Main where

import System.Environment (getArgs)

import Proxy (proxy)
import Client (client)
import DataNode (dataNode)
import NameNode (nameNode)
import NodeInitialization (setupProxy, setupClient, setupNode, setupNameNode)

main :: IO ()
main = do
    prog <- getArgs
    case prog of
        ["proxy",host,port,addr] -> setupProxy proxy host port addr
        ["client",host,port,phost,pport] -> setupClient client host port phost pport
        ["datanode",host,port,addr] -> setupNode dataNode host port addr
        ["namenode",host,port] -> setupNameNode nameNode host port
        _ -> putStrLn "Invalid command line arguments"
