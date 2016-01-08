{-# LANGUAGE DeriveDataTypeable, DeriveGeneric#-}

module Messages where

import Data.Typeable
import GHC.Generics
import Data.Binary (Binary)
import Control.Distributed.Process

type BlockId = Int

data ClientReq = Read FilePath
               | Write FilePath (SendPort (ProcessId, BlockId))
 deriving (Typeable, Generic)

instance Binary ClientReq
