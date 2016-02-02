module Main where

import System.Environment (getArgs)

import Client (client)
import Test (testClient)
import DataNode (dataNode)
import NameNode (nameNode)
import NodeInitialization (setupProxy, setupClient, setupNode, setupNameNode)

main :: IO ()
main = do
    prog <- getArgs
    case prog of
        ["client",host,port] -> setupClient client host port
        ["test",host,port,addr] -> setupNode testClient host port addr
        ["datanode",host,port,addr] -> setupNode dataNode host port addr
        ["namenode",host,port] -> setupNameNode nameNode host port
        _ -> putStrLn "Invalid command line arguments"
