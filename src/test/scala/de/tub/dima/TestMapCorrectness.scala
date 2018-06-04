package de.tub.dima

import de.tub.dima.operators.map.Map_ArrArr_NoCartesian
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import org.scalatest.Tag


/**
  * @author dieutth, 03/06/2018
  * @see documentation at [[http://www.bioinformatics.deib.polimi.it/geco/?try]] for more information
  *      about MAP operator semantic.
  * 
  * Tests for the correctness of GMQL Map operation in different scenarios.
  * The default aggregation function is Count.
  *         
  */
class TestMapCorrectness extends TestBase {


  test("Arr-Arr: No region-duplication in each sample in both dataset"){
    val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/no_region_duplicate_in_both/ref/"
    val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/no_region_duplicate_in_both/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    val expectedResult = Array(
      (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0))),
      (GRecordKey(s12, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0))),
      (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0))),
      (GRecordKey(s02, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0))),

      (GRecordKey(s10, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0))),
      (GRecordKey(s11, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(2))),
      (GRecordKey(s12, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(1))),

      (GRecordKey(s00, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(2))),
      (GRecordKey(s02, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0)))
      ).map(Rep(_)).toSet

    val actualResult = Map_ArrArr_NoCartesian(sc, ref, exp, bin).transformToRow().collect().map(Rep(_)).toSet
    assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))

  }


  test("Arr-Arr: Region dup in REF only"){
    val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_ref_only/ref/"
    val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_ref_only/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    val expectedResult = Array(
      (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2))),

      (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(2))),


      (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(2))),

      (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2)))
    ).map(Rep(_)).toSet

    val actualResult = Map_ArrArr_NoCartesian(sc, ref, exp, bin).transformToRow().collect().map(Rep(_)).toSet
    assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))

  }

  ignore("Arr-Arr: Region dup in EXP only", Tag("ignore")){
//  test("Arr-Arr: Region dup in EXP only", Tag("ignore")){
    val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_exp_only/ref/"
    val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_exp_only/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    val expectedResult = Array(
      (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2))),

      (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(2))),


      (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(2))),

      (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2)))
    ).map(Rep(_)).toSet

    val actualResult = Map_ArrArr_NoCartesian(sc, ref, exp, bin).transformToRow().collect().map(Rep(_)).toSet
    assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))

  }


  ignore("Arr-Arr: Region dup in BOTH dataset", Tag("ignore")){
//  test("Arr-Arr: Region dup in BOTH dataset", Tag("ignore")){
    val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_both/ref/"
    val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_both/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    val expectedResult = Array[GRECORD](
    ).map(Rep(_)).toSet

    val actualResult = Map_ArrArr_NoCartesian(sc, ref, exp, bin).transformToRow().collect().map(Rep(_)).toSet
    assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))

  }



  ignore("Arr-Arr: Record dup in REF only", Tag("ignore")){
//  test("Arr-Arr: Record dup in REF only", Tag("ignore")){
    val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_ref_only/ref/"
    val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_ref_only/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    val expectedResult = Array[GRECORD](
    ).map(Rep(_)).toSet

    val actualResult = Map_ArrArr_NoCartesian(sc, ref, exp, bin).transformToRow().collect().map(Rep(_)).toSet
    assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))

  }




}


