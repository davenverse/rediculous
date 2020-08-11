
package ammonite
package $file.core.src.main.scala.io.chrisdavenport.rediculous.cluster
import _root_.ammonite.interp.api.InterpBridge.{
  value => interp
}
import _root_.ammonite.interp.api.InterpBridge.value.{
  exit
}
import _root_.ammonite.interp.api.IvyConstructor.{
  ArtifactIdExt,
  GroupIdExt
}
import _root_.ammonite.runtime.tools.{
  browse,
  grep,
  time,
  tail
}
import _root_.ammonite.repl.tools.{
  desugar,
  source
}
import _root_.ammonite.main.Router.{
  doc,
  main
}
import _root_.ammonite.repl.tools.Util.{
  pathScoptRead
}


object test{
/*<script>*/import cats.kernel.Hash

import io.chrisdavenport.rediculous.cluster.HashSlot

val out = HashSlot.hashKey("foo")
/*<amm>*/val res_3 = /*</amm>*/println(out)/*</script>*/ /*<generated>*/
def $main() = { scala.Iterator[String]() }
  override def toString = "test"
  /*</generated>*/
}
