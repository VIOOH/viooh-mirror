properties([
    [$class: 'BuildDiscarderProperty', strategy: [
        $class: 'LogRotator', numToKeepStr: '10', artifactNumToKeepStr: '10']],
])

node{
    stage ('Build') {
        checkout scm
        sh "lein do clean, uberjar"
    }

}