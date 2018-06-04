package de.tub.dima

import de.tub.dima.operators.join.ArrArrJoin
import de.tub.dima.operators.map.Map_ArrArr_NoCartesian
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, RegionBuilder}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}

/**
  * @author dieutth, 04/06/2018
  * @see documentation at [[http://www.bioinformatics.deib.polimi.it/geco/?try]] for more information
  *      about JOIN operator semantic.
  *
  * Tests for the correctness of GMQL JOIN operation in different scenarios.
  * Only DLE clause is considered.
  *
  */
class TestJoinCorrectness extends TestBase {
    val distLess = Some(DistLess(0))
    val distGreater = None

    val expectedResultNoRegionDup = Array(

    )
    test("Join Arr-Arr: No region-duplication in each sample in both dataset"){
      val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/no_region_duplicate_in_both/ref/"
      val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/no_region_duplicate_in_both/exp/"
      val ref = loadDataset(refFilePath).transformToSingleMatrix()
      val exp = loadDataset(expFilePath).transformToSingleMatrix()

      val expectedResult = Array(
        (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0.2), GDouble(0.3))),
        (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0.2), GDouble(0.3))),

        (GRecordKey(s11, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.7), GDouble(9.9))),
        (GRecordKey(s11, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.7), GDouble(0.2))),
        (GRecordKey(s12, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.2), GDouble(0.3))),

        (GRecordKey(s00, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.2), GDouble(0.3))),
        (GRecordKey(s01, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.7), GDouble(9.9))),
        (GRecordKey(s01, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.7), GDouble(0.2)))
      ).map(Rep(_)).toSet


      val actualResult = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater)
        .transformToRow().collect().map(Rep(_)).toSet

      assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))

    }


  test("JOIN Arr-Arr: Region dup in REF only"){
    val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_ref_only/ref/"
    val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_ref_only/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    val expectedResult = Array(
      (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2), GDouble(2))),

      (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1), GDouble(1))),
      (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(2), GDouble(2))),


      (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(2), GDouble(2))),

      (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1), GDouble(1))),
      (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2), GDouble(2)))
    ).map(Rep(_)).toSet

    val actualResult = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet
    assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))
  }


  ignore("Arr-Arr: Region dup in EXP only"){
//  test("Arr-Arr: Region dup in EXP only"){
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


  ignore("Arr-Arr: Region dup in BOTH dataset"){
//  test("Arr-Arr: Region dup in BOTH dataset"){
    val refFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_both/ref/"
    val expFilePath = "/home/dieutth/git/MGD-GMQL/src/test/resources/region_dup_in_both/exp/"
    val ref = loadDataset(refFilePath).transformToSingleMatrix()
    val exp = loadDataset(expFilePath).transformToSingleMatrix()

    val expectedResult = Array[GRECORD](
    ).map(Rep(_)).toSet

    val actualResult = Map_ArrArr_NoCartesian(sc, ref, exp, bin).transformToRow().collect().map(Rep(_)).toSet
    assert(expectedResult.subsetOf(actualResult) && actualResult.subsetOf(expectedResult))

  }



  ignore("Arr-Arr: Record dup in REF only"){
//  test("Arr-Arr: Record dup in REF only"){
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
