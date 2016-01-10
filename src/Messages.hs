{-# LANGUAGE DeriveDataTypeable, DeriveGeneric#-}

module Messages where

import Data.Typeable
import GHC.Generics
import Data.Binary (Binary)
import Control.Distributed.Process

type BlockId = Int
type Position = (ProcessId, BlockId)

data HandShake = HandShake
  { sendPort :: SendPort ClientReq
  }
  deriving (Typeable, Generic)

data ClientReq = Read FilePath (SendPort (Maybe Position))
               | Write FilePath (SendPort Position)
 deriving (Typeable, Generic)

instance Binary HandShake
instance Binary ClientReq
