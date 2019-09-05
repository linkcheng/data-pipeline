#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: docker_executor 
@date: 2019-08-08 
"""
import pathlib
import logging
import docker
from docker.errors import ImageNotFound, BuildError, APIError, NotFound
from config.docker_config import (
    BASE_URL,
    VERSION,
    WORK_PATH,
    LOG_PATH,
    DOCKERFILE_TEMPLATE
)

logger = logging.getLogger(__name__)


class DockerExecutor:

    def __init__(self, image_repo, image_tag, container_name, container_cmd,
                 *args, **kwargs):
        self.dockerfile_path = WORK_PATH
        self.dockerfile_name = 'Dockerfile'
        self.image_repository = image_repo
        self.image_tag = image_tag
        self.image_name = f'{self.image_repository}:{self.image_tag}'
        self.container_name = container_name
        self.container_cmd = container_cmd
        self.client = docker.DockerClient(base_url=BASE_URL, version=VERSION)

    def build_dockerfile(self, path=None, template=DOCKERFILE_TEMPLATE):
        path = path or self.dockerfile_path
        logger.info(f'Create dockerfile in {path}')
        full_name = str(pathlib.Path(path, self.dockerfile_name).absolute())
        with open(full_name, 'w+') as f:
            f.write(template)
        logger.info(f'Dockerfile created in {path}')

    def ensure_dockerfile(self):
        if not pathlib.Path(self.dockerfile_path, self.dockerfile_name).exists():
            logger.info(f'Dockerfile not found, then create it')
            self.build_dockerfile()

    def build_image(self):
        self.ensure_dockerfile()

        kwargs = {
            'path': self.dockerfile_path,
            'tag': self.image_name,
            'rm': True,
            'timeout': 60,
        }

        logger.info(f'Build image {self.image_name}')
        try:
            image = self.client.images.build(**kwargs)
        except BuildError as e:
            logger.error(f'Image build error {e}')
            raise

        return image

    def pull_image(self):
        logger.info(f'Pull image {self.image_name}')
        return self.client.images.pull(self.image_repository, self.image_tag)

    def get_image(self):
        logger.info(f'Get image {self.image_name}')
        return self.client.images.get(self.image_name)

    def ensure_image(self):
        try:
            self.get_image()
        except ImageNotFound:
            logger.info(f'Image {self.image_name} not found, then pull it')
            already = False
        else:
            already = True

        if not already:
            try:
                self.pull_image()
            except APIError:
                logger.info(f'Image {self.image_name} not found, then build it')
            else:
                already = True

        if not already:
            self.build_image()
            logger.info(f'Image {self.image_name} built done')

    def create_container(self):
        self.ensure_image()

        volumes = {
            LOG_PATH: {
                'bind': LOG_PATH,
                'mode': 'rw'
            }
        }

        logger.info(f'Create container {self.container_name} based on image {self.image_name} ')
        container = self.client.containers.create(
            self.image_name,
            name=self.container_name,
            volumes=volumes
        )
        logger.info(f'Container {self.container_name} created done')

        return container

    def get_container(self):
        logger.info(f'Get container {self.container_name}')
        return self.client.containers.get(self.container_name)

    def build_and_run_container(self):
        self.ensure_image()

        volumes = {
            LOG_PATH: {
                'bind': LOG_PATH,
                'mode': 'rw'
            }
        }

        logger.info(f'Run container {self.container_name}')
        container = self.client.containers.run(
            self.image_name,
            self.container_cmd,
            name=self.container_name,
            detach=True,
            volumes=volumes
        )

        logger.info(f'Container running id: {container.id}')
        return container.id

    @staticmethod
    def start_container(container):
        if container.status == 'running':
            logger.info(f'Container {container.name} is running')
            return

        logger.info(f'Start container {container.name}')
        container.start()

    @staticmethod
    def stop_container(container):
        if container.status == 'exited':
            logger.info(f'Container {container.name} is already exited')
            return

        logger.info(f'Stop container {container.name}')
        container.stop()

    def run(self):
        try:
            container = self.get_container()
        except NotFound:
            logger.info(f'Container {self.container_name} not exist, build and run it')
            self.build_and_run_container()
        else:
            self.start_container(container)


if __name__ == '__main__':
    from utils.log import configure_logging
    from datetime import date

    configure_logging()
    # 新镜像的仓库名称
    repo = 'python_base_images/data_pipeline_executor'
    # 新镜像 tag
    tag = f'v{date.today().strftime("%Y%m%d")}'

    c_name = 'executor1'
    c_cmd = '--help'
    # c_cmd = '/sbin/init'

    exe = DockerExecutor(repo, tag, c_name, c_cmd)

    # exe.ensure_image()
    exe.run()
