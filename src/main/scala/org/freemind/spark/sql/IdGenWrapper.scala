package org.freemind.spark.sql

import com.tivo.unified.IdGen.gen

class IdGenWrapper extends Serializable {

  def computeGen(identifier: String, key: Int): Long = gen(identifier, key)


}
