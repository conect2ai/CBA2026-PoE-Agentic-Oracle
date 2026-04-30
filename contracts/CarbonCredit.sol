// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/token/ERC721/extensions/ERC721Enumerable.sol";
import "@openzeppelin/contracts/token/ERC721/extensions/ERC721URIStorage.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/Counters.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol"; // NOVO: Para o incentivo do motorista

contract CarbonCredit is ERC721, ERC721Enumerable, ERC721URIStorage, Ownable, ReentrancyGuard {
    using Counters for Counters.Counter;
    Counters.Counter private _tokenIdCounter;

    uint256 public constant GRAMS_PER_TON = 1_000_000;

    // --- ORACLE / POE LOGIC ---
    // Mapping of trusted institutions (Universities, AI Agents) allowed to verify and mint
    mapping(address => bool) public isTrustedOracle;
    
    // On-chain storage of the CO2 saved per token (useful for DeFi integrations)
    mapping(uint256 => uint256) public co2SavedGrams;

    // --- NOVAS VARIÁVEIS PARA REDE BESU (ZERO GAS) ---
    mapping(uint256 => string) public auditReports; // Armazena o texto completo do LLM
    mapping(uint256 => bool) public isRewardClaimed; // Previne que o motorista saque duas vezes
    
    IERC20 public rewardToken; // O token ERC20 de incentivo financeiro (ex: Real Digital, USDC, EcoCoin)
    uint256 public rewardPerGram = 1; // Ex: 1 token (wei) de recompensa pago por cada grama salva

    // Marketplace logic mappings (From your original contract)
    struct Listing {
        address seller;
        uint256 price;
    }
    mapping(uint256 => Listing) public listings;
    // ... (Keep your existing marketplace variables and logic here) ...

    event OracleAdded(address oracle);
    event OracleRemoved(address oracle);
    event CreditMinted(uint256 tokenId, address driver, uint256 co2Grams, string tokenURI);
    event RewardClaimed(uint256 tokenId, address driver, uint256 amount); // NOVO EVENTO

    modifier onlyOracle() {
        require(isTrustedOracle[msg.sender] || msg.sender == owner(), "Not a trusted Oracle");
        _;
    }

    // NOVO: Construtor agora pede o endereço do Token ERC20 usado para recompensar os motoristas
    constructor(address _rewardToken) ERC721("DescarbonUnified", "DSCO2") {
        rewardToken = IERC20(_rewardToken);
    }

    // --- ADMIN FUNCTIONS ---
    function addOracle(address _oracle) external onlyOwner {
        isTrustedOracle[_oracle] = true;
        emit OracleAdded(_oracle);
    }

    function removeOracle(address _oracle) external onlyOwner {
        isTrustedOracle[_oracle] = false;
        emit OracleRemoved(_oracle);
    }

    // --- POE MINTING FUNCTION ---
    /**
     * @dev Called by the AI Agent pipeline after successfully auditing the CSV trip data.
     * @param driver The wallet address of the driver to receive the token.
     * @param co2Grams The verified amount of CO2 saved during the trip.
     * @param _tokenURI The IPFS URI containing the detailed AI Audit JSON and trip metadata.
     * @param _auditReport O laudo completo em texto gerado pelo Oráculo (LLM).
     */
    function mintVerifiedCredit(
        address driver, 
        uint256 co2Grams, 
        string memory _tokenURI,
        string memory _auditReport
    ) 
        external 
        onlyOracle 
        returns (uint256) 
    {
        _tokenIdCounter.increment();
        uint256 newTokenId = _tokenIdCounter.current();

        _mint(driver, newTokenId);
        _setTokenURI(newTokenId, _tokenURI);
        
        // Store the eco-value on-chain
        co2SavedGrams[newTokenId] = co2Grams;
        
        // NOVO: Salvando o relatório em texto na blockchain!
        auditReports[newTokenId] = _auditReport;

        emit CreditMinted(newTokenId, driver, co2Grams, _tokenURI);
        
        return newTokenId;
    }

    // --- LÓGICA DE INCENTIVO AO MOTORISTA ---
    /**
     * @dev Permite que o motorista resgate um incentivo financeiro atrelado à sua economia de CO2.
     * @param tokenId O ID do NFT de Carbono pertencente ao motorista.
     */
    function claimReward(uint256 tokenId) external nonReentrant {
        require(ownerOf(tokenId) == msg.sender, "Apenas o motorista dono do NFT pode sacar");
        require(!isRewardClaimed[tokenId], "O incentivo desta viagem ja foi sacado");
        
        // Exemplo: Salvar 500g de CO2 * 1 token = 500 tokens de recompensa
        uint256 amountToPay = co2SavedGrams[tokenId] * rewardPerGram;
        require(rewardToken.balanceOf(address(this)) >= amountToPay, "O pool de recompensas esta vazio");

        isRewardClaimed[tokenId] = true;
        rewardToken.transfer(msg.sender, amountToPay);

        emit RewardClaimed(tokenId, msg.sender, amountToPay);
    }

    // --- OVERRIDES REQUIRED BY SOLIDITY ---
    function _beforeTokenTransfer(address from, address to, uint256 tokenId, uint256 batchSize)
        internal
        override(ERC721, ERC721Enumerable)
    {
        super._beforeTokenTransfer(from, to, tokenId, batchSize);
    }

    function _burn(uint256 tokenId) internal override(ERC721, ERC721URIStorage) {
        super._burn(tokenId);
        // Clear listing and CO2 data if burned
        if (listings[tokenId].seller != address(0)) {
             delete listings[tokenId];
        }
        delete co2SavedGrams[tokenId];
        delete auditReports[tokenId]; // Limpa a string do relatório
        delete isRewardClaimed[tokenId]; // Limpa a trava do saque
    }

    function tokenURI(uint256 tokenId)
        public
        view
        override(ERC721, ERC721URIStorage)
        returns (string memory)
    {
        return super.tokenURI(tokenId);
    }

    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC721, ERC721Enumerable, ERC721URIStorage)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }
    
    // ... (Keep your existing buyItem, listToken, cancelListing functions below) ...
}