{-# LANGUAGE DeriveDataTypeable, DeriveGeneric#-}

module Messages where

import Data.Typeable
import GHC.Generics
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Lazy as B

type FileName = String
type BlockId = Int
type Position = (ProcessId, BlockId)
type FileData = B.ByteString

data HandShake = HandShake
  { dataNodeId :: ProcessId
  }
  deriving (Typeable, Generic)

data File = File
  { name :: FileName
  , position :: Position
  }
  deriving (Typeable, Generic)

instance Show File where
  show (File n p) = n ++ show p

data ClientReq = ListFiles FilePath (SendPort [File])
               | Read FilePath (SendPort ClientRes)
               | Write FilePath (SendPort ClientRes)
               | Shutdown
  deriving (Typeable, Generic)

type ClientRes = Either ClientError Position

data ClientError = InvalidPathError
                 | FileNotFound
                 | DirectoryNotFound
  deriving (Typeable, Generic, Show)

data CDNReq = CDNRead BlockId (SendPort FileData)
            | CDNWrite BlockId FileData
            | CDNDelete BlockId
  deriving (Typeable, Generic)

instance Binary HandShake
instance Binary ClientError
instance Binary File
instance Binary ClientReq
instance Binary CDNReq
