package de.tub.dima.dimension

case class Coordinate(start: Long, stop: Long, chr: String, strand: Short) {
  override def toString: String = chr + "\t" + start + "\t" + stop + "\t" + strand.toChar
}
