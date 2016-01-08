{-# LANGUAGE TemplateHaskell #-}

module DataNode where

import Control.Distributed.Process
import Control.Distributed.Process.Closure

dataNode :: Process ()
dataNode = undefined

remotable ['dataNode]
