/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.performancetest.models

import com.ligadata.KamanjaBase._
import com.ligadata.KamanjaBase.{ TimeRange, ModelBaseObj, ModelBase, ModelResultBase, TransactionContext, ModelContext }
import com.ligadata.KamanjaBase.{ BaseMsg, BaseContainer, RddUtils, RddDate, BaseContainerObj, MessageContainerBase, RDDObject, RDD }
import system._

object JarvisModel extends ModelBaseObj {
  override def IsValidMessage(msg: MessageContainerBase): Boolean = return msg.isInstanceOf[input]
  override def CreateNewModel(mdlCtxt: ModelContext): ModelBase = return new JarvisModel(mdlCtxt)
  override def ModelName: String = "JarvisModel"
  override def Version: String = "0.0.3"
  override def CreateResultObject(): ModelResultBase = new MappedModelResults()
}

class JarvisModel(mdlCtxt : ModelContext) extends ModelBase(mdlCtxt, JarvisModel) {
  override def execute(emitAllResults:Boolean):ModelResultBase = {
    try{
      val randomNo = scala.util.Random
      val randomNoLimit=randomNo.nextInt(100)
      println("Random no selected is: "+randomNoLimit)
      var jarvis : input =  mdlCtxt.msg.asInstanceOf[input]
      if(randomNoLimit>10000000)
        return null;
      var jarvisRDD=datastore.getRDD.filter(x => x.id==(randomNoLimit))
      var arrayRDDDescription=jarvisRDD.map{ x => (x.description) }.toArray
      var arrayRDDId=jarvisRDD.map{ x => (x.id) }.toArray
      println("Jarvis description is: "+arrayRDDDescription(0))
      var actualResults: Array[Result] = Array[Result](new Result(arrayRDDId(0).toString,arrayRDDDescription(0)))
      return JarvisModel.CreateResultObject().asInstanceOf[MappedModelResults].withResults(actualResults)
    }
    catch{
      case e: Exception => println("exception: "+e.getStackTraceString)
        var actualResults: Array[Result] = Array[Result](new Result("exception","exception"))
        return JarvisModel.CreateResultObject().asInstanceOf[MappedModelResults].withResults(actualResults)
    }
  }
}
