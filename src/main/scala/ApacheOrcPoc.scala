import org.apache.orc.{OrcFile, Reader, RecordReader, TypeDescription}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.typesafe.config.{Config, ConfigValue}
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch

import java.io.IOException
import scala.jdk.CollectionConverters.CollectionHasAsScala

object ApacheOrcPoc extends App {

  /**
   * Use this exception when there is an issue with the job
   *
   * @param message   exception description
   * @param exception inherent exception which cause this exception
   */

  class ORCFileException(message: String, exception: Throwable) extends Exception(message, exception)

  trait ObjectReader[T] {

    /** To read row, and convert data to given case class [[T]]
     *
     * @param row
     *   Map to represent each row in ORC file
     * @param sourceProcessingStage
     *   Source and Processing Stage, used to fill [[T]] object
     * @return
     */
    def getMappedDataToObject(row: Map[String, Option[Any]], sourceProcessingStage: (String, String)): T
  }

  /** ORCFileReader reads an orc file from [[orcFilePath]] and provides data row wise.
   * @param config
   *
   * @param orcFilePath
   *   orc file path in string format This class is currently specific to data types Long, Bytes, Decimal, timestamp, struct, List
   *   of struct.
   */
  class ORCFileReader(config: Config, private val orcFilePath: String) {

    /** @return
     *   object of reader
     */
    def getORCReader: Reader =
      buildOrcReader()

    /** It creates reader to read an orc file from specific path and set all required hadoop configs. Throws ORCFileException in
     * case of any failure.
     *
     * @return
     *   object of Reader
     */
    private def buildOrcReader(): Reader =
      try {
        val filePath   = new Path(orcFilePath)
        val hadoopConf = new Configuration()

        val hadoopOptionsConfig: Config = config.getConfig("hadoopOptions")
        val hadoopOptionsEntrySet       = hadoopOptionsConfig.entrySet().asScala
        hadoopOptionsEntrySet.foreach { eachEntry: java.util.Map.Entry[String, ConfigValue] =>
          hadoopConf.set(eachEntry.getKey, eachEntry.getValue.render().replace("\"", ""))
        }

        val reader = OrcFile.createReader(filePath, OrcFile.readerOptions(hadoopConf))
        reader
      } catch {
        case e: IOException =>
          throw new ORCFileException(e.getLocalizedMessage, e)
      }

    /** @param orcReader
     *   object of Reader
     *
     * @return
     *   object of RecordReader
     */
    def getRecordReader(orcReader: Reader): RecordReader =
      try
        orcReader.rows()
      catch {
        case ex: IOException =>
          throw new ORCFileException(ex.getLocalizedMessage, ex)
      }

    /** @param orcReader
     *   object of Reader
     *
     * @return
     *   object of VectorizedRowBatch
     */
    def getBatch(orcReader: Reader): VectorizedRowBatch =
      getSchema(orcReader).createRowBatch()

    /** provides schema of an orc file
     * @param orcReader
     *   object of Reader
     *
     * @return
     *   object of TypeDescription
     */
    def getSchema(orcReader: Reader): TypeDescription =
      orcReader.getSchema

    /** It returns data of a specific row number [[fileRowNum]] of an orc file and consider batch wise [[readBatch]] if its true
     * otherwise reads the current set batch.
     *
     * @param orcReader
     *   object of Reader
     * @param recordReader
     *   object of RecordReader
     * @param batch
     *   object of VectorizedRowBatch
     * @param fileRowNum
     *   row number to be readed
     * @param readBatch
     *   boolean to read batch wise or not
     *
     * @return
     *   data of Row in Map[String, Option[Any]]
     */
    def readRow(
                 orcReader: Reader,
                 recordReader: RecordReader,
                 batch: VectorizedRowBatch,
                 schema: TypeDescription,
                 fileRowNum: Int,
                 readBatch: Boolean
               ): Map[String, Option[Any]] =
      try {

        if (!readBatch) {
          recordReader.nextBatch(batch)
        }

        if (fileRowNum < orcReader.getNumberOfRows()) {
          val row: Map[String, Option[Any]] = readBatchRow(fileRowNum, Map.empty, batch, schema)
          row
        } else {
          Map.empty[String, Option[Any]]
        }
      } catch {
        case ex: IOException =>
          throw new ORCFileException(ex.getLocalizedMessage, ex)
      }

    /** It returns the value of column of a specific row [[rowNum]] and specific column [[colVec]].
     *
     * @param colVec
     *   object of ColumnVector to be readed
     * @param colType
     *   object of TypeDescription
     * @param rowNum
     *   row number to be readed
     *
     * @return
     *   column value in Option[Any]
     */
    def readColumn(colVec: ColumnVector, colType: TypeDescription, rowNum: Int): Option[Any] =
      if (!colVec.isNull(rowNum)) {
        try {
          val rowNumForRepeating = if (colVec.isRepeating) 0 else rowNum
          Some(colVec.`type` match {
            case ColumnVector.Type.LONG      => readLongVal(colVec, colType, rowNumForRepeating)
            case ColumnVector.Type.BYTES     => readBytesVal(colVec, colType, rowNumForRepeating)
            case ColumnVector.Type.DECIMAL   => readDecimalVal(colVec, rowNumForRepeating)
            case ColumnVector.Type.TIMESTAMP => readTimestampVal(colVec, colType, rowNumForRepeating)
            case ColumnVector.Type.STRUCT    => readStructVal(colVec, colType, rowNumForRepeating)
            case ColumnVector.Type.LIST      => readListVal(colVec, colType, rowNumForRepeating)
          })
        } catch {
          case ex: Exception =>
            throw new ORCFileException("readColumn: unsupported ORC file column type: " + colVec.`type`.name(), ex)
        }

      } else None

    /** It returns true if [[rowIndex]] of [[batch]] is present in orc file else false.
     *
     * @param orcReader
     *   object of Reader
     * @param recordReader
     *   object of RecordReader
     * @param batch
     *   object of VectorizedRowBatch
     * @param rowIndex
     *   row number to be checked
     *
     * @return
     *   Boolean
     */
    def hasNextRow(orcReader: Reader, recordReader: RecordReader, batch: VectorizedRowBatch, rowIndex: Int): Boolean = {
      recordReader.nextBatch(batch)
      if (rowIndex < orcReader.getNumberOfRows) {
        true
      } else {
        false
      }
    }

    /** It reads the column type of list of struct and returns the column value of [[colVec]] and [[rowNum]] from an orc file.
     *
     * @param colVec
     *   object of ColumnVector
     * @param colType
     *   object of TypeDescription
     * @param rowNum
     *   row number to be checked
     *
     * @return
     *   Any
     */
    def readListVal(colVec: ColumnVector, colType: TypeDescription, rowNum: Int = 0): Any = {

      val finalOutput = if (colVec.isNull(rowNum)) {
        List.empty
      } else {
        val listColumnVector =
          try
            colVec match {
              case listColumnVector: ListColumnVector => listColumnVector
            }
          catch {
            case ex: Exception =>
              throw new ORCFileException("readListVal: Invalid child column vector type. Expected list Column Vector.", ex)
          }

        val listChildVector = listColumnVector.child
        val listChildType   = colType.getChildren.get(0)

        try
          listChildVector.`type` match {
            case ColumnVector.Type.STRUCT => readStructListValues(listColumnVector, listChildType, rowNum)
          }
        catch {
          case ex: Exception => throw new ORCFileException("Only struct list value read supported.", ex)
        }

      }
      finalOutput
    }

    /** It reads the column type of struct list and returns the column value of [[listVector]] and @rowNum from an orc file.
     *
     * @param listVector
     *   object of ListColumnVector
     * @param colType
     *   object of TypeDescription
     * @param rowNum
     *   row number to be checked
     *
     * @return
     *   List[Map[String, Any]] Map[String, Any] here fieldName is mapped to the value of the column.
     */
    def readStructListValues(listVector: ListColumnVector, colType: TypeDescription, rowNum: Int): List[Map[String, Any]] = {
      val offset    = listVector.offsets(rowNum).toInt
      val numValues = listVector.lengths(rowNum).toInt
      val structColumnVector = listVector.child match {
        case structVector: StructColumnVector => structVector
        case _ => throw new IllegalArgumentException("Invalid column vector type. Expecting StructColumnVector.")
      }
      readStructListVector(structColumnVector, colType, offset, numValues, 0, List())
    }

    /** It reads the column type of struct and returns the column value of [[colVec]] and [[rowNum]] from an orc file.
     *
     * @param colVec
     *   object of ColumnVector
     * @param colType
     *   object of TypeDescription
     * @param rowNum
     *   row number to be checked
     *
     * @return
     *   Map[String, Option[Any]] Map[String, Any] here fieldName is mapped to the value of the column.
     */
    def readStructVal(colVec: ColumnVector, colType: TypeDescription, rowNum: Int = 0): Map[String, Option[Any]] =
      if (!colVec.isNull(rowNum)) {
        val structVector = colVec match {
          case structVector: StructColumnVector => structVector
          case _ => throw new IllegalArgumentException("Invalid column vector type. Expecting StructColumnVector.")
        }
        val fieldVec     = structVector.fields
        val fieldValList = getFieldList(Map.empty, fieldVec.indices, colType, fieldVec)
        fieldValList
      } else Map.empty

    /** It reads the column type of Bytes and returns the column value of @colVec and [[rowNum]] from an orc file.
     *
     * @param colVec
     *   object of ColumnVector
     * @param colType
     *   object of TypeDescription
     * @param rowNum
     *   row number to be checked
     *
     * @return
     *   String
     */

    def readBytesVal(colVec: ColumnVector, colType: TypeDescription, rowNum: Int = 0): String =
      if (!colVec.isNull(rowNum)) {
        val bytesVector = colVec match {
          case bytesVector: BytesColumnVector => bytesVector
          case _ => throw new IllegalArgumentException("Invalid column vector type. Expecting BytesColumnVector.")
        }
        val columnBytes = bytesVector.vector(rowNum)
        val vecLen      = bytesVector.length(rowNum)
        val vecStart    = bytesVector.start(rowNum)
        val vecCopy     = java.util.Arrays.copyOfRange(columnBytes, vecStart, vecStart + vecLen)
        if (colType.getCategory.equals(TypeDescription.Category.STRING)) {
          new String(vecCopy, StandardCharsets.UTF_8)
        } else {
          vecCopy.toString
        }
      } else null

    /** It reads the column type of timestamp and returns the column value of [[colVec]] and @rowNum from an orc file.
     *
     * @param colVec
     *   object of ColumnVector
     * @param colType
     *   object of TypeDescription
     * @param rowNum
     *   row number to be checked
     *
     * @return
     *   Any
     */
    def readTimestampVal(colVec: ColumnVector, colType: TypeDescription, rowNum: Int = 0): Any =
      if (!colVec.isNull(rowNum)) {
        val timestampVec = colVec match {
          case timestampVec: TimestampColumnVector => timestampVec
          case _ => throw new IllegalArgumentException("Invalid column vector type. Expecting TimestampColumnVector.")
        }
        val nanos     = timestampVec.nanos(rowNum)
        val millisec  = timestampVec.time(rowNum)
        val timestamp = new Timestamp(millisec)
        timestamp.setNanos(nanos)
        if (colType.getCategory == TypeDescription.Category.DATE) {
          new Date(timestamp.getTime)
        } else timestamp
      } else None

    /** It reads the column type of Decimal and returns the column value of [[colVec]] and [[rowNum]] from an orc file.
     *
     * @param colVec
     *   object of ColumnVector
     * @param rowNum
     *   row number to be checked
     *
     * @return
     *   Any
     */
    def readDecimalVal(colVec: ColumnVector, rowNum: Int = 0): Any =
      if (!colVec.isNull(rowNum)) {
        val decimalVec = colVec match {
          case decimalVec: DecimalColumnVector => decimalVec
          case _ => throw new IllegalArgumentException("Invalid column vector type. Expecting DecimalColumnVector.")
        }
        decimalVec.vector(rowNum).getHiveDecimal().bigDecimalValue()
      } else None

    /** It reads the column type of Long and returns the column value of [[colVec]] and [[rowNum]] from an orc file.
     *
     * @param colVec
     *   object of ColumnVector
     * @param colType
     *   object of TypeDescription
     * @param rowNum
     *   row number to be checked
     *
     * @return
     *   Any
     */
    def readLongVal(colVec: ColumnVector, colType: TypeDescription, rowNum: Int = 0): Any =
      if (!colVec.isNull(rowNum)) {
        val longVec = colVec match {
          case longVec: LongColumnVector => longVec
          case _ => throw new IllegalArgumentException("Invalid column vector type. Expecting LongColumnVector.")
        }
        val longVal = longVec.vector(rowNum)
        if (colType.getCategory == TypeDescription.Category.INT) {
          longVal.toInt
        } else if (colType.getCategory == TypeDescription.Category.BOOLEAN) {
          if (longVal == 1) true else false
        } else if (colType.getCategory == TypeDescription.Category.DATE) {
          new Date(longVal)
        } else longVal
      } else None

    /** It is an reccursive function that reads all the column values from the struct list [[structColumnVector]] and map those
     * values to their respective fieldNames and append in a list [[accumulator]]
     *
     * @param structColumnVector
     *   object of StructColumnVector
     * @param childType
     *   object of TypeDescription
     * @param offset
     *   object of TypeDescription
     * @param numValues
     *   row number to be checked
     * @param i
     *   row number to be checked
     * @param accumulator
     *   row number to be checked
     *
     * @return
     *   Any
     */

    @tailrec
    private def readStructListVector(
                                      structColumnVector: StructColumnVector,
                                      childType: TypeDescription,
                                      offset: Int,
                                      numValues: Int,
                                      i: Int,
                                      accumulator: List[Map[String, Any]]
                                    ): List[Map[String, Any]] =
      if (i.equals(numValues)) {
        accumulator
      } else {
        val output = readStructVal(structColumnVector, childType, offset + i)
        readStructListVector(structColumnVector, childType, offset, numValues, i + 1, accumulator.::(output))
      }

    /** It is an reccursive method that reads data of row [[rowNum]] and appends the result into [[row]] [[row]] is of type
     * Map[String, Option[Any]] where all row values get mapped to the row fieldName.
     *
     * @param rowNum
     *   row number to be readed
     * @param row
     *   accumulator at which result get appended
     * @param batch
     *   object of VectorizedRowBatch
     * @param schema
     *   object of TypeDescription
     * @param i
     *   iterator of row from index 0 to rowNum
     *
     * @return
     *   [[row]]
     */
    @tailrec
    private def readBatchRow(
                              rowNum: Int,
                              row: Map[String, Option[Any]],
                              batch: VectorizedRowBatch,
                              schema: TypeDescription,
                              i: Int = 0
                            ): Map[String, Option[Any]] = {
      val numCols = batch.numCols
      if (i.equals(numCols))
        row
      else {
        val cols              = batch.cols
        val colTypes          = schema.getChildren
        val fieldName: String = schema.getFieldNames.get(i)
        val colObj            = readColumn(cols(i), colTypes.get(i), rowNum)
        readBatchRow(rowNum, row + (fieldName -> colObj), batch, schema, i + 1)
      }
    }

    /** @param fieldValList
     *   accumulator at which result get appended
     * @param indices
     *   object of Range of column vector
     * @param colType
     *   object of TypeDescription
     * @param fieldVec
     *   array of column vector
     * @param i
     *   iterator of column from index 0 to range of indices.
     *
     * @return
     *   Map[String, Option[Any]]
     */

    @tailrec
    private def getFieldList(
                              fieldValList: Map[String, Option[Any]],
                              indices: Range,
                              colType: TypeDescription,
                              fieldVec: Array[ColumnVector],
                              i: Int = 0
                            ): Map[String, Option[Any]] =
      if (!indices.contains(i))
        fieldValList
      else {
        val fieldTypes = colType.getChildren.asScala.toList
        val fieldNames = colType.getFieldNames.asScala.toList
        assert(fieldVec.length == fieldTypes.size)
        val fieldObj = readColumn(fieldVec(i), fieldTypes(i), 0)
        getFieldList(fieldValList ++ Map(fieldNames(i) -> fieldObj), indices, colType, fieldVec, i + 1)
      }

  }

}