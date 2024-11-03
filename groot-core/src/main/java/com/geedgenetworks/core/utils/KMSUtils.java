package com.geedgenetworks.core.utils;

import cn.hutool.core.util.StrUtil;
import com.geedgenetworks.common.config.KmsConfig;
import com.geedgenetworks.common.config.ClientSSLConfig;
import com.geedgenetworks.core.pojo.DataEncryptionKey;
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
public class KMSUtils {
    public static final String KMS_TYPE_LOCAL = "local";
    public static final String KMS_TYPE_VAULT = "vault";

    public static DataEncryptionKey getVaultKey(KmsConfig kmsConfig, ClientSSLConfig sslConfig, String identifier) {
        try {
            Vault vault = getVaultClient(kmsConfig, sslConfig);
            String exportKeyPath = constructKeyPath(kmsConfig, identifier);
            LogicalResponse exportResponse = vault.logical().read(exportKeyPath);
            if (exportResponse.getRestResponse().getStatus() == 200) {
                JsonObject keys = exportResponse.getDataObject().get("keys").asObject();
                return new DataEncryptionKey(Base64.getDecoder().decode(StrUtil.trim(keys.get(keys.size() + "").asString(), '"')), keys.size());
            }
        } catch (VaultException e) {
            log.error("Error retrieving vault key from path: {}. ", identifier, e);
        } catch (Exception e) {
            log.error("Unexpected error retrieving vault key.", e);
        }
        return null;
    }

    private static String constructKeyPath(KmsConfig kmsConfig, String identifier) {
        return (CryptoProvider.Algorithm.SM4_GCM96.getIdentifier().equals(identifier)
                ? kmsConfig.getPluginKeyPath()
                : kmsConfig.getDefaultKeyPath()) + "/export/encryption-key/" + identifier;
    }

    public static Vault getVaultClient(KmsConfig kmsConfig, ClientSSLConfig sslConfig) throws VaultException {
        VaultConfig config = new VaultConfig()
                .address(kmsConfig.getUrl())
                .engineVersion(1)
                .sslConfig(configureSSL(sslConfig))
                .build();
        AuthResponse authResponse = Vault.create(config).auth().loginByUserPass(kmsConfig.getUsername(), kmsConfig.getPassword());
        config.token(authResponse.getAuthClientToken());
        return Vault.create(config);
    }

    private static SslConfig configureSSL(ClientSSLConfig clientSSLConfig) throws VaultException {
        boolean verifySSL = clientSSLConfig != null && !clientSSLConfig.getSkipVerification();
        SslConfig sslConfig = new SslConfig().verify(verifySSL).build();
        if (verifySSL) {
            sslConfig.pemFile(new File(clientSSLConfig.getCaCertificatePath()))
                    .clientPemFile(new File(clientSSLConfig.getCertificatePath()))
                    .clientKeyPemFile(new File(clientSSLConfig.getPrivateKeyPath()))
                    .build();
        }
        return sslConfig;
    }
}
