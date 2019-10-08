properties([
    [$class: 'BuildDiscarderProperty', strategy: [
        $class: 'LogRotator', numToKeepStr: '10', artifactNumToKeepStr: '10']],
])



node{
    stage ('Build') {
        checkout scm
        withCredentials([usernamePassword(credentialsId: 'Artifactory', usernameVariable: 'ARTIFACTORY_USR', passwordVariable: 'ARTIFACTORY_PSW')]) {
          sh "lein do clean, check, midje, uberjar"
        }
    }

    stage("Cleanup containers") {
        sh """
          sudo docker images | grep viooh-mirror | awk '{print \$3}' | xargs -I{} sudo docker rmi -f {}
          """
   }


    // container tags: BUILD_NUMBER, master
    stage("Package master") {
        if(env.BRANCH_NAME == 'master'){
        sh """
          sudo docker build . -t viooh-mirror:${env.BUILD_NUMBER}
          sudo docker tag viooh-mirror:${env.BUILD_NUMBER} 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror:${env.BUILD_NUMBER}
          sudo docker tag viooh-mirror:${env.BUILD_NUMBER} 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror:master
          DOCKER_LOGIN="sudo \$(aws ecr get-login --no-include-email --region=eu-west-1 --registry-ids 517256697506)"
          eval "\$DOCKER_LOGIN"
          sudo docker push 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror
          """
        }
   }

    // container tags: TAG_NAME, stable
   stage("Package Tag") {

        // BUILD_NUMBER resets when building tags. DO NOT USE
        if(env.TAG_NAME != null){
        sh """
          sudo docker build . -t viooh-mirror:${env.TAG_NAME}
          sudo docker tag viooh-mirror:${env.TAG_NAME} 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror:${env.TAG_NAME}
          sudo docker tag viooh-mirror:${env.TAG_NAME} 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror:stable
          DOCKER_LOGIN="sudo \$(aws ecr get-login --no-include-email --region=eu-west-1 --registry-ids 517256697506)"
          eval "\$DOCKER_LOGIN"
          sudo docker push 517256697506.dkr.ecr.eu-west-1.amazonaws.com/apps/viooh-mirror
          """
        }
   }

}
