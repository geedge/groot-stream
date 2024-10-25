package com.geedgenetworks.core.utils;

import cn.hutool.core.util.StrUtil;
import com.geedgenetworks.common.config.KmsConfig;
import com.geedgenetworks.common.config.SSLConfig;
import com.geedgenetworks.core.pojo.KmsKey;
import io.github.jopenlibs.vault.SslConfig;
import io.github.jopenlibs.vault.Vault;
import io.github.jopenlibs.vault.VaultConfig;
import io.github.jopenlibs.vault.VaultException;
import io.github.jopenlibs.vault.json.JsonObject;
import io.github.jopenlibs.vault.response.AuthResponse;
import io.github.jopenlibs.vault.response.LogicalResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Base64;

@Slf4j
public class KmsUtils {
    public static final String KMS_TYPE_LOCAL = "local";
    public static final String KMS_TYPE_VAULT = "vault";

    public static KmsKey getVaultKey(KmsConfig kmsConfig, SSLConfig sslConfig, String identifier) throws VaultException {
        Vault vault = getVaultClient(kmsConfig, sslConfig);
        String exportKeyPath;
        if (EncryptionAlgorithmUtils.ALGORITHM_SM4_GCM96_NAME.equals(identifier)) {
            exportKeyPath = kmsConfig.getPluginKeyPath() + "/export/encryption-key/" + identifier;
        } else {
            exportKeyPath = kmsConfig.getDefaultKeyPath() + "/export/encryption-key/" + identifier;
        }
        LogicalResponse exportResponse = vault.logical().read(exportKeyPath);
        if (exportResponse.getRestResponse().getStatus() == 200) {
            JsonObject keys = exportResponse.getDataObject().get("keys").asObject();
            return new KmsKey(Base64.getDecoder().decode(StrUtil.trim(keys.get(keys.size() + "").asString(), '"')), keys.size());
        } else {
            log.error("Get kms key error! code: {} body: {}", exportResponse.getRestResponse().getStatus(), new String(exportResponse.getRestResponse().getBody()));
            return null;
        }
    }

    public static Vault getVaultClient(KmsConfig kmsConfig, SSLConfig sslConfig) throws VaultException {
        String username = kmsConfig.getUsername();
        String password = kmsConfig.getPassword();
        String url = kmsConfig.getUrl();
        boolean skipVerification = true;
        String caCertificatePath = null;
        String certificatePath = null;
        String privateKeyPath = null;
        if (sslConfig != null) {
            skipVerification = sslConfig.getSkipVerification();
            caCertificatePath = sslConfig.getCaCertificatePath();
            certificatePath = sslConfig.getCertificatePath();
            privateKeyPath = sslConfig.getPrivateKeyPath();
        }
        SslConfig vaultSslConfig = new SslConfig().verify(!skipVerification).build();
        if (!skipVerification) {
            vaultSslConfig.pemFile(new File(caCertificatePath))
                    .clientPemFile(new File(certificatePath))
                    .clientKeyPemFile(new File(privateKeyPath))
                    .build();
        }
        VaultConfig config = new VaultConfig()
                .address(url)
                .engineVersion(1)
                .sslConfig(vaultSslConfig)
                .build();
        AuthResponse authResponse = Vault.create(config).auth().loginByUserPass(username, password);
        config.token(authResponse.getAuthClientToken());
        return Vault.create(config);
    }
}
