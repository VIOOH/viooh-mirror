properties([
    [$class: 'BuildDiscarderProperty', strategy: [
        $class: 'LogRotator', numToKeepStr: '30', artifactNumToKeepStr: '30']],
    disableConcurrentBuilds(),
])

def projectName = "viooh-mirror"
def account = "517256697506"

node{
    stage ('Build') {
        checkout scm
        withCredentials([usernamePassword(credentialsId: '4d0f0e2f-e42e-4332-9582-e3a204698a0e', usernameVariable: 'GH_PACKAGES_USR', passwordVariable: 'GH_PACKAGES_PSW')]) { {
          sh "lein do clean, check, midje, uberjar"
        }
    }

    stage("Cleanup images") {
        sh """
          sudo docker images | grep ${projectName} | awk '{print \$3}' | xargs -I{} sudo docker rmi -f {}
          """
   }


    // container tags: BUILD_NUMBER, master
    stage("Package master") {
        if(env.BRANCH_NAME == 'master'){
        sh """
          sudo docker build . -t ${projectName}:${env.BUILD_NUMBER}
          sudo docker tag ${projectName}:${env.BUILD_NUMBER} ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:${env.BUILD_NUMBER}
          sudo docker tag ${projectName}:${env.BUILD_NUMBER} ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:master
          DOCKER_LOGIN="sudo \$(aws ecr get-login --no-include-email --region=eu-west-1 --registry-ids ${account})"
          eval "\$DOCKER_LOGIN"
          sudo docker push ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:master
          sudo docker push ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:${env.BUILD_NUMBER}
          """
        }
   }

    // container tags: TAG_NAME, stable
   stage("Package Tag") {

        // BUILD_NUMBER resets when building tags. DO NOT USE
        if(env.TAG_NAME != null){
        sh """
          sudo docker build . -t ${projectName}:${env.TAG_NAME}
          sudo docker tag ${projectName}:${env.TAG_NAME} ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:${env.TAG_NAME}
          sudo docker tag ${projectName}:${env.TAG_NAME} ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:stable
          DOCKER_LOGIN="sudo \$(aws ecr get-login --no-include-email --region=eu-west-1 --registry-ids ${account})"
          eval "\$DOCKER_LOGIN"
          sudo docker push ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:${env.TAG_NAME}
          sudo docker push ${account}.dkr.ecr.eu-west-1.amazonaws.com/apps/${projectName}:stable
          """
        }
   }

    // DEV Rolling Update
    stage("Rolling Update") {
        if(env.BRANCH_NAME == 'master'){
        sh """
          wget https://github.com/BrunoBonacci/rolling-update/releases/download/0.3.1/rolling-update -O /tmp/rolling-update
          chmod +x /tmp/rolling-update
          /tmp/rolling-update ${projectName}-dev*
          """
        }
   }
}
