/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s

import java.io.File
import java.io.FileReader
import java.util.Locale

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient, OAuthTokenProvider}
import io.fabric8.kubernetes.client.Config.KUBERNETES_KUBECONFIG_FILE
import io.fabric8.kubernetes.client.Config.autoConfigure
import io.fabric8.kubernetes.client.utils.{HttpClientUtils, Utils}
import io.kubernetes.client.util.FilePersister
import io.kubernetes.client.util.KubeConfig
import okhttp3.Dispatcher

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.util.ThreadUtils

private[spark] class SparkOAuthTokenProvider(config: File) extends OAuthTokenProvider {
  val kubeConfig = KubeConfig.loadKubeConfig(new FileReader(config))
  val persister = new FilePersister(config)
  kubeConfig.setPersistConfig(persister)

  def getToken(): String = {
    return kubeConfig.getAccessToken()
  }
}

/**
 * Spark-opinionated builder for Kubernetes clients. It uses a prefix plus common suffixes to
 * parse configuration keys, similar to the manner in which Spark's SecurityManager parses SSL
 * options for different components.
 */
private[spark] object SparkKubernetesClientFactory {

  /**
   * Check if the code is being run from within kubernetes.
   * @return
   */
  def isOnKubernetes(): Boolean = {
    val serviceHost = System.getenv("KUBERNETES_SERVICE_HOST")
    return serviceHost != null && serviceHost.length > 0
  }

  def getHomeDir(): String = {
    val osName = System.getProperty("os.name").toLowerCase(Locale.ROOT)
    if (osName.startsWith("win")) {
      val homeDrive = System.getenv("HOMEDRIVE")
      val homePath = System.getenv("HOMEPATH")
      if (homeDrive != null && !homeDrive.isEmpty() && homePath != null && !homePath.isEmpty()) {
        val homeDir = homeDrive + homePath
        val f = new File(homeDir)
        if (f.exists() && f.isDirectory()) {
          return homeDir
        }
      }
      val userProfile = System.getenv("USERPROFILE")
      if (userProfile != null && !userProfile.isEmpty()) {
        val f = new File(userProfile)
        if (f.exists() && f.isDirectory()) {
          return userProfile
        }
      }
    }
    val home = System.getenv("HOME")
    if (home != null && !home.isEmpty()) {
      val f = new File(home)
      if (f.exists() && f.isDirectory()) {
        return home
      }
    }

    //Fall back to user.home should never really get here
    return System.getProperty("user.home", ".")
  }

  def createKubernetesClient(
      master: String,
      namespace: Option[String],
      kubernetesAuthConfPrefix: String,
      sparkConf: SparkConf,
      defaultServiceAccountToken: Option[File],
      defaultServiceAccountCaCert: Option[File]): KubernetesClient = {
    val oauthTokenFileConf = s"$kubernetesAuthConfPrefix.$OAUTH_TOKEN_FILE_CONF_SUFFIX"
    val oauthTokenConf = s"$kubernetesAuthConfPrefix.$OAUTH_TOKEN_CONF_SUFFIX"
    val oauthTokenFile = sparkConf.getOption(oauthTokenFileConf)
      .map(new File(_))
      .orElse(defaultServiceAccountToken)
    val oauthTokenValue = sparkConf.getOption(oauthTokenConf)
    KubernetesUtils.requireNandDefined(
      oauthTokenFile,
      oauthTokenValue,
      s"Cannot specify OAuth token through both a file $oauthTokenFileConf and a " +
        s"value $oauthTokenConf.")

    val caCertFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CA_CERT_FILE_CONF_SUFFIX")
      .orElse(defaultServiceAccountCaCert.map(_.getAbsolutePath))
    val clientKeyFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CLIENT_KEY_FILE_CONF_SUFFIX")
    val clientCertFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CLIENT_CERT_FILE_CONF_SUFFIX")
    val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonCachedThreadPool("kubernetes-dispatcher"))

    var builder: ConfigBuilder = new ConfigBuilder()
    if (!isOnKubernetes()){
      // Get the kubeconfig file
      var fileName = Utils.getSystemPropertyOrEnvVar(KUBERNETES_KUBECONFIG_FILE, new File(getHomeDir(), ".kube" + File.separator + "config").toString())
      // if system property/env var contains multiple files take the first one based on the environment
      // we are running in (eg. : for Linux, ; for Windows)
      val fileNames = fileName.split(File.pathSeparator)
      if (fileNames.length > 1) {
        fileName = fileNames(0)
      }
      val kubeConfigFile = new File(fileName)

      builder = new ConfigBuilder(autoConfigure(null))
        .withOauthTokenProvider(new SparkOAuthTokenProvider(kubeConfigFile))
    }

    val config = builder.withApiVersion("v1")
      .withMasterUrl(master)
      .withWebsocketPingInterval(0)
      .withOption(oauthTokenValue) {
        (token, configBuilder) => configBuilder.withOauthToken(token)
      }.withOption(oauthTokenFile) {
      (file, configBuilder) =>
        configBuilder.withOauthToken(Files.toString(file, Charsets.UTF_8))
    }.withOption(caCertFile) {
      (file, configBuilder) => configBuilder.withCaCertFile(file)
    }.withOption(clientKeyFile) {
      (file, configBuilder) => configBuilder.withClientKeyFile(file)
    }.withOption(clientCertFile) {
      (file, configBuilder) => configBuilder.withClientCertFile(file)
    }.withOption(namespace) {
      (ns, configBuilder) => configBuilder.withNamespace(ns)
    }.build()

    val baseHttpClient = HttpClientUtils.createHttpClient(config)
    val httpClientWithCustomDispatcher = baseHttpClient.newBuilder()
      .dispatcher(dispatcher)
      .build()
    new DefaultKubernetesClient(httpClientWithCustomDispatcher, config)
  }

  private implicit class OptionConfigurableConfigBuilder(val configBuilder: ConfigBuilder)
    extends AnyVal {

    def withOption[T]
        (option: Option[T])
        (configurator: ((T, ConfigBuilder) => ConfigBuilder)): ConfigBuilder = {
      option.map { opt =>
        configurator(opt, configBuilder)
      }.getOrElse(configBuilder)
    }
  }
}
