package rabid

import cats.syntax.either._
import fs2.Chunk
import scodec.{Attempt, Codec, DecodeResult, Decoder, Encoder, Err}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._

object Encode
{
  def apply[A: Encoder](data: A): Either[Err, BitVector] =
    Encoder[A].encode(data) match {
      case Attempt.Successful(result) =>
        Right(result)
      case Attempt.Failure(error) => Left(error)
    }
}

object Decode
{
  def apply[A: Decoder](data: ByteVector): Either[Err, A] =
    Decoder[A].decode(BitVector(data)) match {
      case Attempt.Successful(DecodeResult(result, _)) =>
        Right(result)
      case Attempt.Failure(error) => Left(error)
    }
}

object BitStream
{
  def extract: PartialFunction[Option[Chunk[Byte]], BitVector] = {
    case Some(a) => BitVector(ByteVector(a.toVector))
  }
}

object ByteConstant
{
  def apply(a: Byte): Codec[Unit] =
    constant(BitVector.fromByte(a)).withContext(s"byte constant $a")

  def as[A](a: A)(b: Byte): Codec[A] = {
    val bits = BitVector.fromByte(b)
    constant(bits).xmap(_ => a, _ => ())
  }
}

object ShortConstant
{
  def apply(a: Short): Codec[Unit] =
    constant(BitVector.fromShort(a)).withContext(s"short constant $a")

  def as[A](a: A)(b: Short): Codec[A] = {
    val bits = BitVector.fromShort(b)
    constant(bits).xmap(_ => a, _ => ())
  }

  def value(a: Short): Codec[Short] =
    as[Short](a)(a)
}

object DecodeString
{
  def apply(data: ByteVector): Attempt[String] =
    Attempt.fromEither(data.decodeUtf8.leftMap(a => Err.General(a.toString, Nil)))
}

object Empty
{
  def apply[A](a: A): Codec[A] =
    constant(BitVector.empty).xmap(_ => a, _ => ())
}

case class LongStringBits(bits: BitVector)

object LongStringBits
{
    def encoder: Encoder[LongStringBits] =
      Encoder((a: BitVector) => ((uint32 ~ bits(a.length)).encode((a.length / 8, a)))).contramap(_.bits)

    def decoder: Decoder[LongStringBits] =
      for {
        length <- uint32
        data <- bits(length * 8).as[LongStringBits].withContext(s"lsb $length")
      } yield data

    implicit def Codec_LongStringBits: Codec[LongStringBits] =
      Codec(encoder, decoder)
}

case class TypedTableItem(name: Field.ShortString, field: Typed)

case class TableItem(name: String, field: Field)

object TableItem
{
  def decodePair(raw: TypedTableItem): TableItem =
    TableItem(raw.name.data, raw.field.data)

  def encodePair(pair: TableItem): TypedTableItem =
    TypedTableItem(Field.ShortString(pair.name), Typed(Typed.typeFor(pair.field), pair.field))

  implicit def Codec_TableItem: Codec[TableItem] =
    (Codec[Field.ShortString] :: Codec[Typed]).as[TypedTableItem]
      .xmap(decodePair _, encodePair _)
}

sealed trait Field

object Field
{
  case class ShortString(data: String)
  extends Field

  object ShortString
  {
    def encoder: Encoder[ShortString] =
      Encoder((a: String) => ((uint8 ~ utf8).encode((a.length.toInt, a)))).contramap(_.data)

    def decoder: Decoder[ShortString] =
      for {
        length <- uint8
        data <- bytes(length).emap(DecodeString(_))
      } yield ShortString(data)

    implicit def Codec_ShortString: Codec[ShortString] =
      Codec(encoder, decoder)
  }

  case class LongString(data: String)
  extends Field

  object LongString
  {
    def encoder: Encoder[LongString] =
      Encoder((a: String) => ((uint32 ~ utf8).encode((a.length.toLong, a)))).contramap(_.data)

    def decoder: Decoder[LongString] =
      for {
        length <- uint32
        data <- bytes(length.toInt).emap(DecodeString(_))
      } yield LongString(data)

    implicit def Codec_LongString: Codec[LongString] =
      Codec(encoder, decoder)
  }

  case class Bool(data: Boolean)
  extends Field

  object Bool
  {
    def encoder: Encoder[Bool] =
      Encoder((a: Boolean) => (byte.encode(if (a) 1 else 0))).contramap(_.data)

    def decoder: Decoder[Bool] =
      Decoder((a: BitVector) => {
        for {
          dec <- byte.decode(a)
          data <- dec match {
            case DecodeResult(b, r) =>
              if (b == 1.toByte) Attempt.Successful(DecodeResult(Bool(true), r))
              else if (b == 0.toByte) Attempt.Successful(DecodeResult(Bool(false), r))
              else Attempt.Failure(Err(s"invalid byte for Bool: $b"))
          }
        } yield data
      })

    implicit def Codec_Bool: Codec[Bool] =
      Codec(encoder, decoder)
  }

  case class Table(data: List[TableItem])
  extends Field

  object Table
  {
    def pairsCodec: Codec[List[TableItem]] =
      list(Codec[TableItem])

    def decodeTable(lsb: LongStringBits): Attempt[Table] =
      pairsCodec.decode(lsb.bits).map(a => Table(a.value))

    def encodeTable(table: Table): Attempt[LongStringBits] =
      pairsCodec.encode(table.data).map(LongStringBits(_))

    implicit def table: Codec[Table] = Codec[LongStringBits].exmap(decodeTable _, encodeTable _)

    def empty: Table = Table(Nil)
  }

  case class Invalid(data: Byte)
  extends Field

  object Invalid
  {
    implicit def Codec_Invalid: Codec[Invalid] =
      byte.as[Invalid]
  }

  implicit def Codec_Field: Codec[Field] = Codec.coproduct[Field].choice

  object codecs
  {
    def field: Codec[Field] = implicitly
    def shortstr: Codec[ShortString] = implicitly
    def longstr: Codec[LongString] = implicitly
    def boolean: Codec[Bool] = implicitly
    def table: Codec[Table] = implicitly
  }
}

case class Typed(tpe: Byte, data: Field)

object Typed
{
  val encoder: Encoder[Typed] =
    (byte :: Codec[Field]).as[Typed]

  val decoder: Decoder[Typed] =
    for {
      tpe <- byte
      data <- decoderFor(tpe)
    } yield Typed(tpe, data)

  implicit def Codec_Typed: Codec[Typed] =
    Codec(encoder, decoder)

  def typeFor: Field => Byte = {
    case Field.ShortString(_) => 's'
    case Field.LongString(_) => 'S'
    case Field.Bool(_) => 't'
    case Field.Table(_) => 'F'
    case _ => 'x'
  }

  def decoderFor: Byte => Decoder[Field] = {
    case 's' => Codec[Field.ShortString]
    case 'S' => Codec[Field.LongString]
    case 't' => Codec[Field.Bool]
    case 'F' => Codec[Field.Table]
    case _ => Codec[Field.Invalid]
  }
}
