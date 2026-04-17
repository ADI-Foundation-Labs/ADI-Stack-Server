variable "IMAGE" {
  default = "ghcr.io/adi-foundation-labs/adi-stack-server"
}

variable "VERSION" {
  default = "dev"
}

variable "SUFFIX" {
  default = "local"
}

variable "PLATFORMS" {
  default = "linux/amd64"
}

variable "CACHE_REF" {
  default = ""
}

target "zksync-os-server" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = split(",", PLATFORMS)
  tags       = ["${IMAGE}:${VERSION}-${SUFFIX}"]
  cache-from = CACHE_REF == "" ? [] : ["type=registry,ref=${CACHE_REF}"]
  cache-to   = CACHE_REF == "" ? [] : ["type=registry,ref=${CACHE_REF},mode=max"]
  labels = {
    "org.opencontainers.image.source"  = "https://github.com/ADI-Foundation-Labs/ADI-Stack-Server"
    "org.opencontainers.image.version" = VERSION
  }
}
