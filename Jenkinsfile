pipeline {
    agent any

    stages {
        stage('Build Maven artifact and installation package') {
            steps {
                withMaven(
                    maven: 'Maven 3.3.3',
                    globalMavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1435422191538') {
                        sh "mvn clean package -P${env.MAVEN_PROFILES}"
                }
            }
        }
        stage('Deploy Maven artifacts') {
            steps {
                withMaven(
                    maven: 'Maven 3.3.3',
                    globalMavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1435422191538') {
                        sh "mvn deploy -DskipTests -DaltDeploymentRepository=${env.MAVEN_REPOSITORY} -P${env.MAVEN_PROFILES}"
                }
            }
        }
    }
}
