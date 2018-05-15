#!/usr/bin/env groovy
node {
    checkout scm
    withEnv(['PATH+EXTRA=$PATH:/usr/local/bin']) {
        if("$env.BRANCH_NAME" == 'master') {
            build 'kafka-clients-enhanced'
        }
        else {
            withMaven(jdk: '1.8', maven: '3.5.0') {
                sh "mvn clean install"
            }
        }
    }

}