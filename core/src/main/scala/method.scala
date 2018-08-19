package rabid

import scodec.Codec
import scodec.codecs._

import Field.{ShortString, LongString, Table, Bool}
import Field.codecs._

case class ClassId[A](id: Short)

object ClassId
{
  def connection[A]: ClassId[A] = ClassId(10)

  def channel[A]: ClassId[A] = ClassId(20)

  def exchange[A]: ClassId[A] = ClassId(40)

  def queue[A]: ClassId[A] = ClassId(50)

  def basic[A]: ClassId[A] = ClassId(60)
}

case class MethodId[A](id: Short)

sealed trait Method

object Method
{
  object connection
  {
    case class Start(major: Int, minor: Int, serverProperties: Table)
    extends Method

    object Start
    {
      implicit val codec: Codec[Start] =
        (int8 :: int8 :: table.withContext("serverProperties")).as[Start].withContext("start")

      implicit val ClassId_Start: ClassId[Start] =
        ClassId.connection

      implicit val MethodId_Start: MethodId[Start] =
        MethodId(10)
    }

    case class StartOk(clientProperties: Table, mechanism: ShortString, response: LongString, locale: ShortString)
    extends Method

    object StartOk
    {
      implicit val codec: Codec[StartOk] = (table :: shortstr :: longstr :: shortstr).as[StartOk]

      implicit val ClassId_StartOk: ClassId[StartOk] =
        ClassId.connection

      implicit val MethodId_StartOk: MethodId[StartOk] =
        MethodId(11)
    }

    case class Secure(challenge: LongString)
    extends Method

    object Secure
    {
      implicit val codec: Codec[Secure] = longstr.as[Secure]

      implicit val ClassId_Secure: ClassId[Secure] =
        ClassId.connection

      implicit val MethodId_Secure: MethodId[Secure] =
        MethodId(20)
    }

    case class Tune(channelMax: Short, frameMax: Int, heartbeat: Short)
    extends Method

    object Tune
    {
      implicit val codec: Codec[Tune] = (short16 :: int32 :: short16).as[Tune]

      implicit val ClassId_Tune: ClassId[Tune] =
        ClassId.connection

      implicit val MethodId_Tune: MethodId[Tune] =
        MethodId(30)
    }

    case class TuneOk(channelMax: Short, frameMax: Int, heartbeat: Short)
    extends Method

    object TuneOk
    {
      implicit val codec: Codec[TuneOk] = (short16 :: int32 :: short16).as[TuneOk]

      implicit val ClassId_TuneOk: ClassId[TuneOk] =
        ClassId.connection

      implicit val MethodId_TuneOk: MethodId[TuneOk] =
        MethodId(31)
    }

    case class Open(vhost: ShortString, capabilities: ShortString, insist: Bool)
    extends Method

    object Open
    {
      implicit val codec: Codec[Open] = (shortstr :: shortstr :: boolean).as[Open]

      implicit val ClassId_Open: ClassId[Open] =
        ClassId.connection

      implicit val MethodId_Open: MethodId[Open] =
        MethodId(40)
    }

    case class OpenOk(knownHosts: ShortString)
    extends Method

    object OpenOk
    {
      implicit val codec: Codec[OpenOk] =
        shortstr.as[OpenOk]

      implicit val ClassId_OpenOk: ClassId[OpenOk] =
        ClassId.connection

      implicit val MethodId_OpenOk: MethodId[OpenOk] =
        MethodId(41)
    }

    case class Close(replyCode: Short, replyText: ShortString, classId: Short, methodId: Short)
    extends Method

    object Close
    {
      implicit val codec: Codec[Close] = (short16 :: shortstr :: short16 :: short16).as[Close]

      implicit val ClassId_Close: ClassId[Close] =
        ClassId.connection

      implicit val MethodId_Close: MethodId[Close] =
        MethodId(50)
    }

    case object CloseOk
    extends Method
    {
      implicit val codec: Codec[CloseOk.type] =
        Empty(CloseOk)

      implicit val ClassId_CloseOk: ClassId[CloseOk.type] =
        ClassId.connection

      implicit val MethodId_CloseOk: MethodId[CloseOk.type] =
        MethodId(51)
    }
  }

  object channel
  {
    case class Open(outOfBand: ShortString)
    extends Method

    object Open
    {
      implicit val codec: Codec[Open] =
        shortstr.as[Open]

      implicit val ClassId_Open: ClassId[Open] =
        ClassId.channel

      implicit val MethodId_Open: MethodId[Open] =
        MethodId(10)
    }

    case class OpenOk(channelId: LongString)
    extends Method

    object OpenOk
    {
      implicit val codec: Codec[OpenOk] =
        longstr.as[OpenOk]

      implicit val ClassId_OpenOk: ClassId[OpenOk] =
        ClassId.channel

      implicit val MethodId_OpenOk: MethodId[OpenOk] =
        MethodId(11)
    }
  }

  object exchange
  {
    case class Declare(
      ticket: Short,
      exchange: ShortString,
      tpe: ShortString,
      passive: Boolean,
      durable: Boolean,
      internal: Boolean,
      autoDelete: Boolean,
      nowait: Boolean,
      arguments: Table,
    )
    extends Method

    object Declare
    {
      implicit val codec: Codec[Declare] =
        (short16 :: shortstr :: shortstr :: bool :: bool :: bool :: bool :: bool :: table).as[Declare]

      implicit val ClassId_Declare: ClassId[Declare] =
        ClassId.exchange

      implicit val MethodId_Declare: MethodId[Declare] =
        MethodId(10)
    }

    case object DeclareOk
    extends Method
    {
      implicit val codec: Codec[DeclareOk.type] =
        Empty(DeclareOk)

      implicit val ClassId_DeclareOk: ClassId[DeclareOk.type] =
        ClassId.exchange

      implicit val MethodId_DeclareOk: MethodId[DeclareOk.type] =
        MethodId(11)
    }
  }

  object queue
  {
    case class Declare(
      ticket: Short,
      queue: ShortString,
      passive: Boolean,
      durable: Boolean,
      exclusive: Boolean,
      autoDelete: Boolean,
      nowait: Boolean,
      arguments: Table,
    )
    extends Method

    object Declare
    {
      implicit val codec: Codec[Declare] =
        (short16 :: shortstr :: bool :: bool :: bool :: bool :: bool :: table).as[Declare]

      implicit val ClassId_Declare: ClassId[Declare] =
        ClassId.queue

      implicit val MethodId_Declare: MethodId[Declare] =
        MethodId(10)
    }

    case class DeclareOk(queue: ShortString, messageCount: Int, consumerCount: Int)
    extends Method

    object DeclareOk
    {
      implicit val codec: Codec[DeclareOk] =
        (shortstr :: int32 :: int32).as[DeclareOk]

      implicit val ClassId_DeclareOk: ClassId[DeclareOk] =
        ClassId.queue

      implicit val MethodId_DeclareOk: MethodId[DeclareOk] =
        MethodId(11)
    }

    case class Bind(
      ticket: Short,
      queue: ShortString,
      exchange: ShortString,
      routingKey: ShortString,
      nowait: Boolean,
      arguments: Table,
    )
    extends Method

    object Bind
    {
      implicit val codec: Codec[Bind] =
        (short16 :: shortstr :: shortstr :: shortstr :: bool :: table).as[Bind]

      implicit val ClassId_Bind: ClassId[Bind] =
        ClassId.queue

      implicit val MethodId_Bind: MethodId[Bind] =
        MethodId(20)
    }

    case object BindOk
    extends Method
    {
      implicit val codec: Codec[BindOk.type] =
        Empty(BindOk)

      implicit val ClassId_BindOk: ClassId[BindOk.type] =
        ClassId.queue

      implicit val MethodId_BindOk: MethodId[BindOk.type] =
        MethodId(21)
    }
  }

  object basic
  {
    case class Publish(
      ticket: Short,
      exchange: ShortString,
      routingKey: ShortString,
      mandatory: Boolean,
      immediate: Boolean,
    )
    extends Method

    object Publish
    {
      implicit val codec: Codec[Publish] =
        (short16 :: shortstr :: shortstr :: bool :: bool).as[Publish]

      implicit val ClassId_Publish: ClassId[Publish] =
        ClassId.basic

      implicit val MethodId_Publish: MethodId[Publish] =
        MethodId(40)
    }

    case class Get(
      ticket: Short,
      queue: ShortString,
      noack: Boolean,
    )
    extends Method

    object Get
    {
      implicit val codec: Codec[Get] =
        (short16 :: shortstr :: bool).as[Get]

      implicit val ClassId_Get: ClassId[Get] =
        ClassId.basic

      implicit val MethodId_Get: MethodId[Get] =
        MethodId(70)
    }

    case class GetOk(
      deliveryTag: Long,
      redelivered: Bool,
      exchange: ShortString,
      routingKey: ShortString,
      messageCount: Int,
    )
    extends Method

    object GetOk
    {
      implicit val codec: Codec[GetOk] =
        (int64 :: boolean :: shortstr :: shortstr :: int32).as[GetOk]

      implicit val ClassId_GetOk: ClassId[GetOk] =
        ClassId.basic

      implicit val MethodId_GetOk: MethodId[GetOk] =
        MethodId(71)
    }

    case class GetEmpty(clusterId: ShortString)
    extends Method

    object GetEmpty
    {
      implicit val codec: Codec[GetEmpty] =
        shortstr.as[GetEmpty]

      implicit val ClassId_GetEmpty: ClassId[GetEmpty] =
        ClassId.basic

      implicit val MethodId_GetEmpty: MethodId[GetEmpty] =
        MethodId(72)
    }

    case class GetResponse(message: Either[ClassMethod[GetEmpty], ClassMethod[GetOk]])

    object GetResponse
    {
      implicit def Codec_GetResponse: Codec[GetResponse] =
        fallback(Codec[ClassMethod[GetEmpty]], Codec[ClassMethod[GetOk]]).as[GetResponse]
    }
  }

  implicit def Discriminated_Method: Discriminated[Method, (Short, Short)] =
    Discriminated(short16 ~ short16)

  implicit def Discriminator_Method_Variant[A <: Method]
  (implicit classId: ClassId[A], methodId: MethodId[A])
  : Discriminator[Method, A, (Short, Short)] =
    Discriminator(classId.id, methodId.id)

  implicit def Codec_Method: Codec[Method] =
    Codec.coproduct[Method].auto
}

case class ClassMethod[A](method: A)

object ClassMethod
{
  implicit def codec[A: Codec](implicit classId: ClassId[A], methodId: MethodId[A]): Codec[ClassMethod[A]] =
    (ShortConstant(classId.id) :: ShortConstant(methodId.id) :: Codec[A]).as[ClassMethod[A]]
}
