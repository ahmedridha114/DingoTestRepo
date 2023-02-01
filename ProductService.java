package de.telekom.magbus.ecc.productinventory.service;

import de.telekom.magbus.ecc.productinventory.domain.Product;
import de.telekom.magbus.ecc.productinventory.domain.ProductPrice;
import de.telekom.magbus.ecc.productinventory.domain.ProductRelationship;
import de.telekom.magbus.ecc.productinventory.domain.SearchCriteria;
import de.telekom.magbus.ecc.productinventory.domain.enumeration.ProductStatusType;
import de.telekom.magbus.ecc.productinventory.errors.InvalidProductDeleteStatusException;
import de.telekom.magbus.ecc.productinventory.errors.InvalidProductInitialStatusException;
import de.telekom.magbus.ecc.productinventory.errors.InvalidStatusTransitionException;
import de.telekom.magbus.ecc.productinventory.errors.ProductNotFoundException;
import de.telekom.magbus.ecc.productinventory.errors.ProductsNotFoundException;
import de.telekom.magbus.ecc.productinventory.repository.ProductRelationshipRepository;
import de.telekom.magbus.ecc.productinventory.repository.ProductRepository;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

public class ProductService {
  public static final String ROOT = "root";
  public static final String BUNDLED = "bundled";
  public static final String CSV_DELIMITER = ",";
  private final List<String> csvHeaders =
      List.of("Product name", "Contract number", "One time charge", "Monthly recurring charge",
          "Start date", "Duration");
  private final ProductRepository productRepository;
  private final ProductRelationshipRepository productRelationshipRepository;
  private final String href;

  private static final Logger LOG = LoggerFactory.getLogger(ProductService.class);

  public ProductService(
      ProductRepository productRepository,
      ProductRelationshipRepository productRelationshipRepository,
      String hrefBasePath,
      String hrefProductPath) {
    Assert.notNull(productRepository, "productRepository must not be null");
    Assert.notNull(productRelationshipRepository, "productRelationshipRepository must not be null");
    Assert.notNull(hrefBasePath, "hrefBasePath must not be null");
    Assert.notNull(hrefProductPath, "hrefProductPath must not be null");
    this.productRepository = productRepository;
    this.productRelationshipRepository = productRelationshipRepository;
    this.href = hrefBasePath + hrefProductPath + "/%s";
  }

  public Product insert(Product product, List<Product> referencedProducts) {
    if (product.getStatus() == null) {
      product.setStatus(ProductStatusType.CREATED);
    }
    if (product.getStatus() != ProductStatusType.CREATED) {
      throw new InvalidProductInitialStatusException();
    }

    product.setIdent(generateUUID());
    setContractNumberForRootProduct(product);
    product.setHref(String.format(href, product.getIdent()));
    return this.storeProduct(product, referencedProducts);
  }

  private void setContractNumberForRootProduct(Product product) {
    if (ROOT.equals(product.getAtBaseType())) {
      product.setContractNumber(getNextContractNumber());
    }
  }

  private String getNextContractNumber() {
    return String.format("GKP%07d", productRepository.getNextSeriesId());
  }

  public Product storeProduct(Product product, List<Product> referencedProducts) {
    Assert.notNull(product, "product must not be null");
    Assert.notNull(referencedProducts, "referencedProducts must not be null");

    if (product.getProductRelationships() != null) {
      if (referencedProducts.isEmpty()) {
        product.getProductRelationships().clear();
      }

      product.getProductRelationships().forEach(productRelationship -> {
        var ident = productRelationship.getProductRef().getIdent();
        var exception = new ProductsNotFoundException(List.of(ident));
        var existingProductRef = referencedProducts
            .stream()
            .filter(refProd -> refProd.getIdent().equals(ident))
            .findFirst()
            .orElseThrow(() -> exception);
        productRelationship.setProductRef(existingProductRef);
      });

      if (ROOT.equals(product.getAtBaseType())) {
        setRootProductReference(product, product);
      }
    }

    return productRepository.save(product);
  }

  public Product getProductByIdent(String id) {
    return productRepository.findProductByIdent(id).orElseThrow(ProductNotFoundException::new);
  }

  public List<Product> getProductsByIdents(Set<String> ids) {
    var products = productRepository.findProductsByIdents(ids);

    if (products.size() != ids.size()) {
      throw new ProductsNotFoundException(findMissingProductIds(products, ids));
    }

    return products;
  }

  public void validateStatusTransition(ProductStatusType previous, ProductStatusType next) {
    if (previous == next) {
      return;
    }

    if (previous == ProductStatusType.CREATED && next == ProductStatusType.ACTIVE) {
      return;
    }

    if (previous == ProductStatusType.CREATED && next == ProductStatusType.ABORTED) {
      return;
    }

    if (previous == ProductStatusType.ACTIVE && next == ProductStatusType.TERMINATED) {
      return;
    }

    if (previous == ProductStatusType.TERMINATED && next == ProductStatusType.ACTIVE) {
      return;
    }

    if (previous == ProductStatusType.ACTIVE && next == ProductStatusType.PENDINGTERMINATE) {
      return;
    }

    if (previous == ProductStatusType.PENDINGTERMINATE && next == ProductStatusType.TERMINATED) {
      return;
    }

    if (previous == ProductStatusType.PENDINGTERMINATE && next == ProductStatusType.ACTIVE) {
      return;
    }

    throw new InvalidStatusTransitionException(previous, next);
  }

  public List<Product> searchProduct(SearchCriteria searchCriteria) {
    return productRepository.findProductsBySearchCriteria(searchCriteria);
  }

  private String generateUUID() {
    return UUID.randomUUID().toString();
  }

  private List<String> findMissingProductIds(List<Product> products, Set<String> ids) {
    return ids.stream()
        .filter(productId -> products.stream()
            .noneMatch(refProduct -> refProduct.getIdent().equals(productId)))
        .collect(Collectors.toList());
  }

  private void setRootProductReference(Product product, Product rootProduct) {
    LOG.debug("Setting root product relationship to: {}", product.getIdent());

    // add relationship to root for everyone except self if they don't have on already
    if (!product.getIdent().equals(rootProduct.getIdent())) {
      if (!hasRelationshipToRoot(product)) {
        var relationshipToRoot = new ProductRelationship()
            .productRef(rootProduct)
            .relationshipType(ROOT);
        product.addProductRelationship(relationshipToRoot);
        LOG.debug("Product root relationship to: {} SET!", product.getIdent());
      }
    }

    // traverse child relationships recursively
    product.getProductRelationships().forEach(rel -> {
      if (ROOT.equals(rel.getRelationshipType())) {
        return;
      }
      setRootProductReference(rel.getProductRef(), rootProduct);
    });
  }

  private boolean hasRelationshipToRoot(Product product) {
    return product.getProductRelationships()
      .stream()
      .anyMatch(rel -> ROOT.equals(rel.getRelationshipType()));
  }

  @Transactional
  public InputStreamResource exportProduct(Set<String> ids) {
    List<String> dataLines = new ArrayList<>();
    dataLines.add(String.join(CSV_DELIMITER, csvHeaders));
    for (Product product : getProductsByIdents(ids)) {
      dataLines.add(mapProductToCsvRow(product));
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(outputStream);
    dataLines.forEach(printWriter::println);
    printWriter.close();
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(outputStream.toByteArray());
    return new InputStreamResource(byteArrayInputStream);
  }

  private String mapProductToCsvRow(Product product) {
    StringJoiner productDataJoiner = new StringJoiner(CSV_DELIMITER);
    productDataJoiner.add(StringUtils.defaultString(product.getName()));
    productDataJoiner.add(StringUtils.defaultString(product.getContractNumber()));
    productDataJoiner.add(getCsvOtc(product.getProductPrices()));
    productDataJoiner.add(getCsvMrc(product.getProductPrices()));
    productDataJoiner.add(getCsvStartDate(product));
    productDataJoiner.add(getCsvDuration(product));
    return productDataJoiner.toString();
  }

  private String getCsvOtc(Set<ProductPrice> productPrices) {
    return productPrices.stream()
        .filter(productPrice -> "OTC".equals(productPrice.getPriceType())).findFirst()
        .filter(price -> price.getPrice() != null)
        .map(price -> (price.getPrice().getDutyFreeAmountValue() != null
            ? price.getPrice().getDutyFreeAmountValue().toString() : "")
            + (StringUtils.defaultString(price.getPrice().getDutyFreeAmountUnit()))).orElse("");
  }

  private String getCsvMrc(Set<ProductPrice> productPrices) {
    return productPrices.stream()
        .filter(productPrice -> "MRC".equals(productPrice.getPriceType())).findFirst()
        .filter(price -> price.getPrice() != null)
        .map(price -> (price.getPrice().getDutyFreeAmountValue() != null
            ? price.getPrice().getDutyFreeAmountValue().toString() : "")
            + (StringUtils.defaultString(price.getPrice().getDutyFreeAmountUnit()))).orElse("");
  }

  private String getCsvStartDate(Product product) {
    if (product.getStartDate() == null) {
      return "";
    }
    return product.getStartDate().toLocalDate().format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));
  }

  private String getCsvDuration(Product product) {
    if (product.getStartDate() == null || product.getTerminationDate() == null) {
      return "";
    }
    Period durationPeriod = Period.between(product.getStartDate().toLocalDate(),
        product.getTerminationDate().toLocalDate());
    return (durationPeriod.toTotalMonths() > 0 ? durationPeriod.toTotalMonths() + "M" :
        "") + (durationPeriod.getDays() > 0 ? durationPeriod.getDays() + "D" : "");
  }

  public void deleteProduct(String productId) {
    Optional<Product> existingProduct = productRepository.findProductByIdent(productId);
    var product = existingProduct.orElseThrow(ProductNotFoundException::new);
    if (!ProductStatusType.TERMINATED.equals(product.getStatus())) {
      throw new InvalidProductDeleteStatusException();
    }
    if (!ROOT.equals(product.getAtBaseType())) {
      // productRefId is child product ID
      LOG.debug("Delete product relationship with relationshipType {} and productRefId {}.",
          BUNDLED, product.getId());
      productRelationshipRepository
          .deleteProductRelationshipByTypeAndProductRefId(BUNDLED, product.getId());
    }
    deleteProductAndChildProducts(product);
  }


  private void deleteProductAndChildProducts(Product product) {
    var childProducts = product.getProductRelationships().stream()
        .filter(productRelationship -> BUNDLED.equals(productRelationship.getRelationshipType()))
        .collect(
            Collectors.toList());
    childProducts.forEach(
        productRelationship -> deleteProductAndChildProducts(productRelationship.getProductRef()));
    LOG.debug("Delete product with ident {} from database.", product.getIdent());
    productRepository.delete(product);
  }

  public void terminateExpiredProducts() {
    LOG.debug("Terminate expired products.");
    productRepository.terminateExpiredProducts();
  }
}
