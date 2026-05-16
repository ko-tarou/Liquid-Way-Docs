// Top-level build file. LEAP SDK requires AGP 8.13.0+ and Kotlin 2.3.0+.
plugins {
    id("com.android.application") version "8.13.2" apply false
    id("org.jetbrains.kotlin.android") version "2.3.21" apply false
    id("org.jetbrains.kotlin.plugin.compose") version "2.3.21" apply false
    // KSP 2.3.x uses unified versioning aligned with the Kotlin 2.3 line
    // (the old "<kotlin>-<ksp>" scheme was dropped). 2.3.8 targets Kotlin 2.3.x.
    id("com.google.devtools.ksp") version "2.3.8" apply false
}
