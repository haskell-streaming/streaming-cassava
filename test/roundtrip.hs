{-# LANGUAGE DeriveGeneric, FlexibleContexts, MultiParamTypeClasses, RankNTypes,
             ScopedTypeVariables #-}

{- |
   Module      : Main
   Description : Round-trip property testing
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Main (main) where

import Streaming.Cassava

import Streaming.Prelude (Of, Stream, each, toList_)

import Test.Hspec                (describe, hspec)
import Test.Hspec.QuickCheck     (prop)
import Test.QuickCheck           (Arbitrary(..))
import Test.QuickCheck.Instances ()

import           Control.Monad.Except (MonadError, runExcept)
import           Data.Text            (Text)
import qualified Data.Vector          as V
import           GHC.Generics         (Generic)

--------------------------------------------------------------------------------

main :: IO ()
main = hspec $ do
  describe "Plain records" $ do
    prop "Just data" (useType encodeDecode)
    prop "With headers" (useType encodeDecodeHeader)
  describe "Named records" $ do
    prop "Default order" (useType encodeDecodeNamed)
    prop "Reversed order" (useType encodeDecodeNamedReordered)

encodeDecode :: (FromRecord a, ToRecord a, Eq a) => [a] -> Bool
encodeDecode = encodeDecodeWith (decode NoHeader . encode Nothing)

encodeDecodeHeader :: (DefaultOrdered a, FromRecord a, ToRecord a, Eq a)
                      => [a] -> Bool
encodeDecodeHeader = encodeDecodeWith (decode HasHeader . encodeDefault)

encodeDecodeNamed :: (DefaultOrdered a, FromNamedRecord a, ToNamedRecord a, Eq a)
                     => [a] -> Bool
encodeDecodeNamed = encodeDecodeWith (decodeByName . encodeByNameDefault)

encodeDecodeNamedReordered :: forall a. (DefaultOrdered a, FromNamedRecord a, ToNamedRecord a, Eq a)
                              => [a] -> Bool
encodeDecodeNamedReordered = encodeDecodeWith (decodeByName . encodeByName hdr)
  where
    hdr = V.reverse (headerOrder (undefined :: a))

encodeDecodeWith :: (Eq a)
                    => (forall m r. (MonadError CsvParseException m) => Stream (Of a) m r -> Stream (Of a) m r)
                    -> [a] -> Bool
encodeDecodeWith f as = either (const False) (as==)
                        . runExcept
                        . toList_
                        . f
                        . each
                        $ as

useType :: ([Test] -> r) -> [Test] -> r
useType = id

data Test = Test
  { columnA            :: !Int
  , longer_column_name :: !Text
  , mebbe              :: !(Maybe Double)
  } deriving (Eq, Show, Read, Generic)

-- DeriveAnyClass doesn't work with these types because of the Maybe

instance FromRecord Test
instance ToRecord Test
instance DefaultOrdered Test
instance FromNamedRecord Test
instance ToNamedRecord Test

instance Arbitrary Test where
  arbitrary = Test <$> arbitrary <*> arbitrary <*> arbitrary
