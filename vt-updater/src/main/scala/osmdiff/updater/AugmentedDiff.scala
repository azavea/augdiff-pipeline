package osmdiff.updater

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.json.{DeserializationException, JsNumber, JsObject, JsString, JsValue, RootJsonReader}

case class AugmentedDiff(
                          changeset: Long,
                          id: Long,
                          elementType: String,
                          timestamp: DateTime,
                          uid: Long,
                          user: String,
                          version: Int,
                          tags: Map[String, String]
                        ) {
  val elementId: String = elementType match {
    case "node" => s"n$id"
    case "way" => s"w$id"
    case "relation" => s"r$id"
    case _ => id.toString
  }
}

object AugmentedDiff {

  implicit object AugmentedDiffFormat extends RootJsonReader[AugmentedDiff] {
    def read(value: JsValue): AugmentedDiff =
      value match {
        case obj: JsObject =>
          val fields = obj.fields

          val changeset = fields.get("changeset") match {
            case Some(JsString(v)) => v.toLong
            case Some(JsNumber(v)) => v.toLong
            case Some(v) => throw DeserializationException(s"'changeset' must be a number, got $v")
          }

          val id = fields.get("id") match {
            case Some(JsString(v)) => v.toLong
            case Some(JsNumber(v)) => v.toLong
            case Some(v) => throw DeserializationException(s"'id' must be a number, got $v")
          }

          val elementType = fields.get("type") match {
            case Some(JsString(v)) => v
            case Some(v) => throw DeserializationException(s"'type' must be a string, got $v")
          }

          val timestamp = fields.get("timestamp") match {
            case Some(JsString(v)) => ISODateTimeFormat.dateTimeParser().parseDateTime(v)
            case Some(v) => throw DeserializationException(s"'type' must be a string, got $v")
          }

          val uid = fields.get("uid") match {
            case Some(JsString(v)) => v.toLong
            case Some(JsNumber(v)) => v.toLong
            case Some(v) => throw DeserializationException(s"'uid' must be a number, got $v")
          }

          val user = fields.get("user") match {
            case Some(JsString(v)) => v
            case Some(v) => throw DeserializationException(s"'user' must be a string, got $v")
          }

          val version = fields.get("version") match {
            case Some(JsString(v)) => v.toInt
            case Some(JsNumber(v)) => v.toInt
            case Some(v) => throw DeserializationException(s"'version' must be a number, got $v")
          }

          val tags = fields.get("tags") match {
            case Some(JsObject(o)) => o.mapValues {
              case JsString(v) => v
              case v => throw DeserializationException(s"tag value must be a string, got $v")
            }
            case Some(v) => throw DeserializationException(s"'tags' must be an object, got $v")
          }

          AugmentedDiff(changeset, id, elementType, timestamp, uid, user, version, tags)
        case _ => throw DeserializationException(s"'properties' is required")
      }
  }

}