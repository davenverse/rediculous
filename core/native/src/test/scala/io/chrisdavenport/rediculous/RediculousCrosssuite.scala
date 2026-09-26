package io.chrisdavenport.rediculous

// Previously overrode munitIORuntime with epollcat's EpollRuntime, which
// supplied a Native event loop before cats-effect had one. cats-effect 3.6
// brought that in-house and epollcat never published for Scala Native 0.5, so
// the default runtime is now correct here and this matches the js-jvm variant.
trait RediculousCrossSuite extends munit.CatsEffectSuite
