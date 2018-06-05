package de.tub.dima

import de.tub.dima.operators.join.{ArrArrJoin, ArrArrJoin_Multimatrix, ArrArrJoin_NoCartesian}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, RegionBuilder}
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

    /**
    *
    * ================================================NO_REGION_DUP ================================================
    *
    ** REF:
    * S0, chr1	1	20	*	0.2	0.3
    * S0, chr1	1	3	*	0.2	0.7
    * S1, chr1	1	3	*	0.7	0.2
    * S1, chr1	5	94	*	0.2	0.2
    *
    ** EXP:
    * S0, chr1	2	5	*	0.2	0.3
    * S1, chr1	5	100	*	0.7	9.9
    * S1, chr1	4	70	*	0.7	0.2
    * S2, chr1	50	101	*	0.2	0.3
    */
  val expectedResultNoRegionDup = Array(
    (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0.2), GDouble(0.3))),
    (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0.2), GDouble(0.3))),

    (GRecordKey(s11, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.7), GDouble(9.9))),
    (GRecordKey(s11, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.7), GDouble(0.2))),
    (GRecordKey(s12, "chr1", 5, 94, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.2), GDouble(0.3))),

    (GRecordKey(s00, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.2), GDouble(0.3))),
    (GRecordKey(s01, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.7), GDouble(9.9))),
    (GRecordKey(s01, "chr1", 1, 20, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.7), GDouble(0.2)))
  ).map(Rep(_)).toSet


//  ignore("Join Arr-Arr-Normal: No region-duplication in BOTH") {
      test("Join Arr-Arr-Normal: No region-duplication in BOTH"){
    val ref = loadDataset(refFilePath_NoRegionDup).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_NoRegionDup).transformToSingleMatrix()
    val actualResult = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater)
      .transformToRow().collect().map(Rep(_)).toSet

        assert(expectedResultNoRegionDup.subsetOf(actualResult) && actualResult.subsetOf(expectedResultNoRegionDup))
  }

      test("Join Arr-Arr-NoCartesian: No region-duplication in BOTH"){
//  ignore("Join Arr-Arr-NoCartesian: No region-duplication in BOTH") {
    val ref = loadDataset(refFilePath_NoRegionDup).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_NoRegionDup).transformToSingleMatrix()
    val actualResult = ArrArrJoin_NoCartesian(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater)
      .transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultNoRegionDup.subsetOf(actualResult) && actualResult.subsetOf(expectedResultNoRegionDup))
  }


  test("Join Arr-Arr-Multimatrix: No region-duplication in BOTH") {
    val refMulti = loadDataset(refFilePath_NoRegionDup).transformToMultiMatrix()
    val exp = loadDataset(expFilePath_NoRegionDup).transformToSingleMatrix()
    val actualResult = ArrArrJoin_Multimatrix(refMulti, exp, bin, RegionBuilder.LEFT, distLess, distGreater)
      .transformToRow().collect().map(Rep(_)).toSet
//    actualResult.foreach(println)

    assert(expectedResultNoRegionDup.subsetOf(actualResult) && actualResult.subsetOf(expectedResultNoRegionDup))
  }


  /**
    *
    * ================================================REGION_DUP_IN_REF_ONLY ================================================
    *
    * * REF:
    *
    * S0, chr1	1	7	*	1	1
    * S0, chr1	1	3	*	2	2
    *
    * S1, chr1	1	3	*	1	1
    * S1, chr1	1	3	*	2	2
    *
    * * EXP:
    * S0, chr1	2	5	*	1	1
    *
    * S1, chr1	1	3	*	1	1
    * S1, chr1	2	4	*	2	2
    */

  val expectedResultRegionDupInREF = Array(
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

    test("JOIN Arr-Arr-Normal: Region dup in REF only"){
//  ignore("JOIN Arr-Arr-Normal: Region dup in REF only") {
    val ref = loadDataset(refFilePath_RegionDupInREF).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RegionDupInREF).transformToSingleMatrix()
    val actualResult = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRegionDupInREF.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInREF))
  }

//  ignore("JOIN Arr-Arr-NoCartesian: Region dup in REF only") {
      test("JOIN Arr-Arr-NoCartesian: Region dup in REF only"){
    val ref = loadDataset(refFilePath_RegionDupInREF).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RegionDupInREF).transformToSingleMatrix()
    val actualResult = ArrArrJoin_NoCartesian(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRegionDupInREF.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInREF))
  }

  test("JOIN Arr-Arr-MultiMatrix: Region dup in REF only") {
    val refMulti = loadDataset(refFilePath_RegionDupInREF).transformToMultiMatrix()
    val exp = loadDataset(expFilePath_RegionDupInREF).transformToSingleMatrix()
    val actualResult = ArrArrJoin_Multimatrix(refMulti, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet
//    actualResult.foreach(println)

    assert(expectedResultRegionDupInREF.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInREF))
  }


  /**
    *
    * ================================================REGION_DUP_IN_EXP_ONLY ================================================
    *
    * * REF:
    * S0, chr1	1	7	*	1	1
    * S0, chr1	1	3	*	2	2
    *
    * * EXP:
    * S0  chr1  2 6 * 1 1 //intersect with both
    * S0  chr1  2 6 * 2 2 //intersect with both, dup region
    * S0  chr1  4 8 * 8 8 //intersect with only one
    * S0  chr1  4 8 * 9 9 //intersect with only 1, dup region
    * S0  chr1  8 9 * 9 9 // no-intersect
    *
    * S1  chr1  2 5 * 5 5 //intersect with both
    * S1  chr1  2 6 * 6 6 //intersect with both, dup region with S0
    * S1  chr1  8 9 8 7 7 //no-intersect
    * S1  chr1  4 8 * 2 2 //intersect with only one, dup region with S0
    * S1  chr1  4 9 * 8 8 //intersect with only one, no dup region with S1
    *
    */

  val expectedResultRegionDupInEXP = Array(
    (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(1), GDouble(1))),
    (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2), GDouble(2))),
    (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(8), GDouble(8))),
    (GRecordKey(s00, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(9), GDouble(9))),

    (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(5), GDouble(5))),
    (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(6), GDouble(6))),
    (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(2), GDouble(2))),
    (GRecordKey(s01, "chr1", 1, 7, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(8), GDouble(8))),

    (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(1), GDouble(1))),
    (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(2), GDouble(2))),

    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(5), GDouble(5))),
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(2), GDouble(2), GDouble(6), GDouble(6)))

  ).map(Rep(_)).toSet

//  ignore("JOIN Arr-Arr-Normal: Region dup in EXP only") {
  test("JOIN Arr-Arr-Normal: Region dup in EXP only") {
    val ref = loadDataset(refFilePath_RegionDupInEXP).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RegionDupInEXP).transformToSingleMatrix()
    val actualResult = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRegionDupInEXP.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInEXP))

  }


//  ignore("JOIN Arr-Arr-NoCartesian: Region dup in EXP only") {
  test("JOIN Arr-Arr-NoCartesian: Region dup in EXP only") {
    val ref = loadDataset(refFilePath_RegionDupInEXP).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RegionDupInEXP).transformToSingleMatrix()
    val actualResult = ArrArrJoin_NoCartesian(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRegionDupInEXP.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInEXP))

  }

//  ignore("JOIN Arr-Arr-Multimatrix: Region dup in EXP only") {
  test("JOIN Arr-Arr-Multimatrix: Region dup in EXP only") {
    val refMulti = loadDataset(refFilePath_RegionDupInEXP).transformToMultiMatrix()
    val exp = loadDataset(expFilePath_RegionDupInEXP).transformToSingleMatrix()
    val actualResult = ArrArrJoin_Multimatrix(refMulti, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRegionDupInEXP.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInEXP))

  }



  /**
    *
    * ================================================REGION_DUP_IN_BOTH ================================================
    *
    * * REF:
    * S0, chr1	1	3	*	0.2	0.3
    * S0, chr1	1	3	*	0.2	0.7
    *
    * S1, chr1	1	3	*	0.7	0.2
    * S1, chr1	2	4	*	0.2	0.2
    * 
    * * EXP:
    * S0, chr1	1	3	*	0.2	0.3
    * S0, chr1	1	3	*	0.2	0.7
    *
    * S1, chr1	1	3	*	0.7	0.2
    * S1, chr1	2	4	*	0.2	0.2
    *
    */

  val expectedResultRegionDupInBOTH = Array(
    (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.2), GDouble(0.3))),
    (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.2), GDouble(0.7))),
    (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0.2), GDouble(0.3))),
    (GRecordKey(s00, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0.2), GDouble(0.7))),
    
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.2), GDouble(0.2))),
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.3), GDouble(0.7), GDouble(0.2))),
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0.2), GDouble(0.2))),
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.2), GDouble(0.7), GDouble(0.7), GDouble(0.2))),

    (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0.2), GDouble(0.3))),
    (GRecordKey(s10, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0.2), GDouble(0.7))),
    (GRecordKey(s10, "chr1", 2, 4, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.2), GDouble(0.3))),
    (GRecordKey(s10, "chr1", 2, 4, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.2), GDouble(0.7))),

    (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0.2), GDouble(0.2))),
    (GRecordKey(s11, "chr1", 1, 3, '*'), Array[GValue](GDouble(0.7), GDouble(0.2), GDouble(0.7), GDouble(0.2))),
    (GRecordKey(s11, "chr1", 2, 4, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.2), GDouble(0.2))),
    (GRecordKey(s11, "chr1", 2, 4, '*'), Array[GValue](GDouble(0.2), GDouble(0.2), GDouble(0.7), GDouble(0.2)))

  ).map(Rep(_)).toSet

  test("JOIN Arr-Arr-Normal: Region dup in BOTH") {
//  ignore("JOIN Arr-Arr-Normal: Region dup in BOTH") {
    val ref = loadDataset(refFilePath_RegionDupInBOTH).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RegionDupInBOTH).transformToSingleMatrix()
    val actualResult = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRegionDupInBOTH.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInBOTH))

  }

//  ignore("JOIN Arr-Arr-NoCartesian: Region dup in BOTH") {
  test("JOIN Arr-Arr-NoCartesian: Region dup in BOTH") {
    val ref = loadDataset(refFilePath_RegionDupInBOTH).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RegionDupInBOTH).transformToSingleMatrix()
    val actualResult = ArrArrJoin_NoCartesian(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRegionDupInBOTH.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInBOTH))

  }

//    ignore("JOIN Arr-Arr-Multimatrix: Region dup in BOTH") {
  test("JOIN Arr-Arr-Multimatrix: Region dup in BOTH") {
      val refMulti = loadDataset(refFilePath_RegionDupInBOTH).transformToMultiMatrix()
      val exp = loadDataset(expFilePath_RegionDupInBOTH).transformToSingleMatrix()
      val actualResult = ArrArrJoin_Multimatrix(refMulti, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet
    
      assert(expectedResultRegionDupInBOTH.subsetOf(actualResult) && actualResult.subsetOf(expectedResultRegionDupInBOTH))
  }

  /**
    *
    * ================================================RECORD-DUP-IN-REF ================================================
    *
    ** REF:
    * S0, chr1	1	3	*	1	1
    * S0, chr1	1	3	*	1	1
    * S0, chr1	1	5	*	5	5
    *
    ** EXP:
    * S0, chr1	50	100	*	100	100
    *
    * S1, chr1	1	6	*	6	6
    * S1, chr1	2	4	*	4	4
    */

  val expectedResultRecordDupREF = Array(
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(6), GDouble(6))),
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(4), GDouble(4))),
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(6), GDouble(6))),
    (GRecordKey(s01, "chr1", 1, 3, '*'), Array[GValue](GDouble(1), GDouble(1), GDouble(4), GDouble(4))),

    (GRecordKey(s01, "chr1", 1, 5, '*'), Array[GValue](GDouble(5), GDouble(5), GDouble(6), GDouble(6))),
    (GRecordKey(s01, "chr1", 1, 5, '*'), Array[GValue](GDouble(5), GDouble(5), GDouble(4), GDouble(4)))

  ).map(Rep(_)).toSet

  test("JOIN Arr-Arr-Normal: Record dup in REF only"){
  //ignore("JOIN Arr-Arr-Normal: Record dup in REF only"){
    val ref = loadDataset(refFilePath_RecordDupInREF).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RecordDupInREF).transformToSingleMatrix()
    val actualResult_1 = ArrArrJoin(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRecordDupREF.subsetOf(actualResult_1) && actualResult_1.subsetOf(expectedResultRecordDupREF))
  }

  test("JOIN Arr-Arr-NoCartesian: Record dup in REF only"){
    //ignore("JOIN Arr-Arr-Normal: Record dup in REF only"){
    val ref = loadDataset(refFilePath_RecordDupInREF).transformToSingleMatrix()
    val exp = loadDataset(expFilePath_RecordDupInREF).transformToSingleMatrix()
    val actualResult_1 = ArrArrJoin_NoCartesian(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRecordDupREF.subsetOf(actualResult_1) && actualResult_1.subsetOf(expectedResultRecordDupREF))
  }

  test("JOIN Arr-Arr-MultiMatrix: Record dup in REF only"){
//  ignore("JOIN Arr-Arr-MultiMatrix: Record dup in REF only"){
    val ref = loadDataset(refFilePath_RecordDupInREF).transformToMultiMatrix()
    val exp = loadDataset(expFilePath_RecordDupInREF).transformToSingleMatrix()
    val actualResult_1 = ArrArrJoin_Multimatrix(ref, exp, bin, RegionBuilder.LEFT, distLess, distGreater).transformToRow().collect().map(Rep(_)).toSet

    assert(expectedResultRecordDupREF.subsetOf(actualResult_1) && actualResult_1.subsetOf(expectedResultRecordDupREF))
  }


}