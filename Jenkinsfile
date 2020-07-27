pipeline {
    agent {
        kubernetes {
            label "jenkins-maven-${UUID.randomUUID().toString()}"
            inheritFrom 'maven-java11'
        }
    }
    environment {
        ORG = 'RiskFocus'
        APP_NAME = 'flink-generic'
        CHARTMUSEUM_CREDS = credentials('chartmuseum-secret')
        CHART_REPOSITORY = "http://${CHARTMUSEUM_CREDS}@jenkins-x-chartmuseum:8080"

        SONAR_OPTS = ' '
    }
    stages {
        stage('CI Build and push snapshot') {
            when {
                changeRequest()
            }
            environment {
                PREVIEW_VERSION = "1.0-${BRANCH_NAME}-SNAPSHOT"
                PREVIEW_NAMESPACE = "$APP_NAME-$BRANCH_NAME".toLowerCase()
                HELM_RELEASE = "$PREVIEW_NAMESPACE".toLowerCase()
            }
            steps {
                container('maven') {
                    withMaven(publisherStrategy: 'EXPLICIT', mavenOpts: MAVEN_OPTS) {
                        sh "mvn versions:set -DnewVersion=PREVIEW_VERSION"
                        sh "mvn clean install"

                        // Only works with the webhook enabled
                        // timeout(time: 1, unit: 'HOURS') {
                        //   waitForQualityGate abortPipeline: true
                        // }

                        // TestNG
                        step([$class: 'Publisher', reportFilenamePattern: '**/testng-results.xml'])

                    }
                }
            }
        }

        stage('FeatureBranch Build and push snapshot') {
            when {
                not {
                    anyOf {
                        branch 'master'
                        branch pattern: "release-.*", comparator: "REGEXP"
                        changeRequest()
                    }
                }
            }
            environment {
                PREVIEW_VERSION = "1.0-${BRANCH_NAME.replace('/', '-')}-SNAPSHOT"
                PREVIEW_NAMESPACE = "$APP_NAME-$BRANCH_NAME".toLowerCase()
                HELM_RELEASE = "$PREVIEW_NAMESPACE".toLowerCase()
            }
            steps {
                container('maven') {
                    withMaven(publisherStrategy: 'EXPLICIT', mavenOpts: MAVEN_OPTS) {
                        sh "mvn versions:set -DnewVersion=$PREVIEW_VERSION"
                        sh "mvn clean deploy"
                    }
                }
            }
        }

        stage('Build Release') {
            when {
                anyOf {
                    branch 'master'
                    branch pattern: "release-.*", comparator: "REGEXP"
                }
            }
            steps {
                container('maven') {
                    // ensure we're not on a detached head
                    sh "git checkout $BRANCH_NAME"

                    sh "git config --global credential.helper store"
                    sh "jx step git credentials"

                    // so we can retrieve the version in later steps
                    sh "echo \$(jx-release-version -same-release) > VERSION"
                    sh "mvn versions:set -DnewVersion=\$(cat VERSION)"

                    script {
                        currentBuild.displayName = readFile('VERSION')
                    }

                    sh "jx step tag --version \$(cat VERSION)"
                    sh "mvn clean deploy"

                    // TestNG
                    step([$class: 'Publisher', reportFilenamePattern: '**/testng-results.xml'])
                }
            }
        }
    }
}
