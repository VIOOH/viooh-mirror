properties([
    [$class: 'BuildDiscarderProperty', strategy: [
        $class: 'LogRotator', numToKeepStr: '10', artifactNumToKeepStr: '10']],
])



node{
    stage ('Build') {
        checkout scm: 
//        withCredentials([usernamePassword(credentialsId: 'Artifactory', usernameVariable: 'ARTIFACTORY_USR', passwordVariable: 'ARTIFACTORY_PSW')]) {
//          sh "lein do clean, check, midje, uberjar"
//        }
    }

    stage("Package") {
//        if(env.BRANCH_NAME == 'master'){
//        sh """
//          sudo docker build . -t viooh-mirror:${env.BUILD_NUMBER}
//          sudo docker tag viooh-mirror:${env.BUILD_NUMBER} 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror:${env.BUILD_NUMBER}
//          sudo docker tag viooh-mirror:${env.BUILD_NUMBER} 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror:master
//          DOCKER_LOGIN="sudo \$(aws ecr get-login --no-include-email --region=eu-west-1 --registry-ids 517256697506)"
//          eval "\$DOCKER_LOGIN"
//          sudo docker push 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror
//          """
//        }
   }

stage("Push tag") {         

        sh """
           echo BRANCH:${env.BRANCH_NAME}
           echo TAG: ${env.TAG_NAME}
           """
   }

}
