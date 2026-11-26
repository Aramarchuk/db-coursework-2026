plugins {
    kotlin("jvm") version "2.4.21"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(25)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("started", "passed", "skipped", "failed")
    }
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}

application {
    mainClass.set("aramarchuk.keyValueBase.MainKt")
}
