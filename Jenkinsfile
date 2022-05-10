pipeline {
    environment {
        registry1 = "rohitkatlaa/dataline_pipeline_creation:latest"
        registry2 = "rohitkatlaa/dataline_pipeline_execution:latest"
        registryCredential = 'dockerhub_id'
        dockerImage1 = ''
        dockerImage2 = ''
    }
    // The “agent” section configures on which nodes the pipeline can be run. 
    // Specifying “agent any” means that Jenkins will run the job on any of the 
    // available nodes.
		agent any 
    stages {
        stage('Git Pull') {
            steps {
                // Get code from a GitHub repository
                // Make sure to add your own git url and credentialsId
                git url: 'https://github.com/rohitkatlaa/dataline.git', branch: 'master', credentialsId: 'jenkins-ansible'
            }
        }
        stage('installing requirements for testing') {
            steps {
                sh 'pip3 install -r pipeline_execution/requirements.txt'
            }
        }
        stage('Testing') {
            steps {
                sh 'cd pipeline_creation; python manage.py test; cd ../pipeline_execution; python manage.py test; python operations_test.py;'
            }
        }
        stage('Django check') {
            steps {
                sh 'cd pipeline_creation; python manage.py check; cd ../pipeline_creation; python manage.py check;'
            }
        }
        stage('Building Image') {
            steps {
                script {
                    // dockerImage = docker.build registry + ":latest"
                    dockerImage1 = docker.build(registry1, "./pipeline_creation")
                    dockerImage2 = docker.build(registry2, "./pipeline_execution")
                }
            }
        }
        stage('Push Image') {
            steps {
                script {
                    docker.withRegistry( '', registryCredential ) {
                        dockerImage1.push()
                    }
                    docker.withRegistry( '', registryCredential ) {
                        dockerImage2.push()
                    }
                }
            }
        }
        stage('Cleaning up') {
            steps {
                sh "docker rmi $registry1"
                sh "docker rmi $registry2"
            }
        }
        stage('Ansible Deploy') {
            steps {
                ansiblePlaybook colorized: true, disableHostKeyChecking: true, installation: 'Ansible',inventory: 'inventory', playbook: 'playbook_docker.yml'
            }
        }
        stage('Post Production Check') {
            steps {
                sh "python post_production_check.py"
            }
        }
    }
}