package io.chrisdavenport.rediculous

import epollcat.unsafe.EpollRuntime

trait RediculousCrossSuite extends munit.CatsEffectSuite {
  override def munitIORuntime = EpollRuntime.global
}