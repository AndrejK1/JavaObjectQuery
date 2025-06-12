plugins {
    id("java")
}

group = "org.anderejk"
version = "1.0.0"

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    compileOnly("org.projectlombok:lombok:1.18.38")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    annotationProcessor("org.projectlombok:lombok:1.18.38")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")
    implementation("org.jline:jline-reader:3.25.0")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}